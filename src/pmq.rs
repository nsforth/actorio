use crate::{AsSocketId, MaybeSocketOwner, ActorioContext, SocketHolder, SocketId};
use mio::event::Source;
use mio::unix::SourceFd;
use mio::{Interest, Registry, Token};
use std::ffi::CString;
use std::io::{Error, ErrorKind};
use std::os::unix::io::RawFd;

type PMQReceiveHandler<'a, A> =
    Box<dyn FnMut(&mut A, &mut ActorioContext<'a, A>, &PMQId, Result<&[u8], Error>) + 'a>;

#[derive(Hash, PartialEq, Eq, Debug)]
pub struct PMQId(SocketId);

impl AsSocketId for PMQId {
    fn as_socket_id(&self) -> &SocketId {
        &self.0
    }
}

impl From<SocketId> for PMQId {
    fn from(socket_id: SocketId) -> Self {
        PMQId(socket_id)
    }
}

pub struct PMQ<'a, A> {
    pmq_fd: PMQFd,
    on_receive: Option<PMQReceiveHandler<'a, A>>,
}

impl<'a, A> PMQ<'a, A> {
    pub fn with_path(path: &str) -> PMQInit {
        PMQInit(path)
    }

    pub fn unlink(pmq_path: &str) -> Result<(), Error> {
        let path = CString::new(pmq_path)?;
        let result = unsafe { libc::mq_unlink(path.as_ptr()) };
        libc_result_cvt(result as libc::c_int).map(|_| ())
    }

    fn new<'p>(
        pmq_path: &'p str,
        flags: libc::c_int,
        on_receive: Option<PMQReceiveHandler<'a, A>>,
    ) -> Result<Self, Error> {
        assert!(
            pmq_path.len() > 2,
            "PosixMQ path should looks like '/somename' but yours is too short: {}",
            pmq_path
        );
        assert_eq!(
            pmq_path.chars().next(),
            Some('/'),
            "PosixMQ path should looks like '/somename' but yours has no root: {}",
            pmq_path
        );
        assert_eq!(
            pmq_path.chars().filter(|chr| *chr == '/').count(),
            1,
            "PosixMQ path should looks like '/somename' but yours has path hierarchy: {}",
            pmq_path
        );
        let path = CString::new(pmq_path)?;
        let mq_attr: *const libc::mq_attr = std::ptr::null();
        let result = unsafe {
            libc::mq_open(
                path.as_ptr(),
                libc::O_NONBLOCK | libc::O_CREAT | flags,
                0o666,
                mq_attr,
            )
        };
        Ok(PMQ {
            pmq_fd: PMQFd(libc_result_cvt(result)?),
            on_receive,
        })
    }

    pub fn send(&self, buf: &[u8]) -> Result<usize, Error> {
        let result = unsafe { libc::mq_send(self.pmq_fd.0, buf.as_ptr() as *mut i8, buf.len(), 0) };
        libc_result_cvt(result).map(|r| r as usize)
    }

    pub(crate) fn process_receive(
        act_ctx: &mut ActorioContext<'a, A>,
        application: &mut A,
        pmq_id: PMQId,
        buf: &mut Vec<u8>,
    ) {
        while let Some(pmq) = act_ctx.try_get_socket(&pmq_id) {
            match pmq.receive(buf) {
                Ok(count) => {
                    let mut on_receive = pmq.on_receive.take().unwrap();
                    on_receive(application, act_ctx, &pmq_id, Ok(&buf[0..count]));
                    if let Some(pmq) = act_ctx.try_get_socket(&pmq_id) {
                        pmq.on_receive = Some(on_receive);
                    } else {
                        break;
                    }
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                Err(e) if e.kind() == ErrorKind::Interrupted => continue,
                Err(e) => {
                    let mut on_receive = pmq.on_receive.take().unwrap();
                    on_receive(application, act_ctx, &pmq_id, Err(e));
                    if let Some(pmq) = act_ctx.try_get_socket(&pmq_id) {
                        pmq.on_receive = Some(on_receive);
                    } else {
                        break;
                    }
                }
            }
        }
    }

    fn receive(&self, buf: &mut [u8]) -> Result<usize, Error> {
        let result = unsafe {
            libc::mq_receive(
                self.pmq_fd.0,
                buf.as_mut_ptr() as *mut i8,
                crate::BUF_SIZE,
                std::ptr::null_mut(),
            )
        };
        libc_result_cvt(result as libc::c_int).map(|r| r as usize)
    }
}

fn libc_result_cvt(result: libc::c_int) -> Result<libc::c_int, Error> {
    if result == -1 {
        let err_code = unsafe { *libc::__errno_location() };
        Err(Error::from_raw_os_error(err_code))
    } else {
        Ok(result)
    }
}

struct PMQFd(RawFd);

impl Drop for PMQFd {
    fn drop(&mut self) {
        unsafe { libc::close(self.0) };
    }
}

impl<'a, A> Source for PMQ<'a, A> {
    fn register(
        &mut self,
        registry: &Registry,
        token: Token,
        interests: Interest,
    ) -> Result<(), Error> {
        registry.register(&mut SourceFd(&self.pmq_fd.0), token, interests)
    }

    fn reregister(
        &mut self,
        registry: &Registry,
        token: Token,
        interests: Interest,
    ) -> Result<(), Error> {
        registry.reregister(&mut SourceFd(&self.pmq_fd.0), token, interests)
    }

    fn deregister(&mut self, registry: &Registry) -> Result<(), Error> {
        registry.deregister(&mut SourceFd(&self.pmq_fd.0))
    }
}

pub struct PMQInit<'p>(&'p str);

impl<'p> PMQInit<'p> {
    pub fn on_receive<'a, A>(
        self,
        on_receive: impl FnMut(&mut A, &mut ActorioContext<A>, &PMQId, Result<&[u8], Error>) + 'a,
    ) -> PMQInitWithReceiveHandler<'a, 'p, A> {
        PMQInitWithReceiveHandler {
            path: self.0,
            on_receive: Box::new(on_receive),
        }
    }

    pub fn open_writeonly<'a, A>(
        self,
        act_ctx: &mut ActorioContext<'a, A>,
    ) -> Result<PMQId, Error> {
        act_ctx
            .register_socket_holder(SocketHolder::new(
                false,
                false,
                From::from(PMQ::new(self.0, libc::O_WRONLY, None)?),
            ))
            .map(From::from)
    }
}

pub struct PMQInitWithReceiveHandler<'a, 'p, A> {
    path: &'p str,
    on_receive: PMQReceiveHandler<'a, A>,
}

impl<'a, 'p, A> PMQInitWithReceiveHandler<'a, 'p, A> {
    pub fn open_readonly(self, act_ctx: &mut ActorioContext<'a, A>) -> Result<PMQId, Error> {
        act_ctx
            .register_socket_holder(SocketHolder::new(
                true,
                false,
                From::from(PMQ::new(self.path, libc::O_RDONLY, Some(self.on_receive))?),
            ))
            .map(From::from)
    }

    pub fn open(self, act_ctx: &mut ActorioContext<'a, A>) -> Result<PMQId, Error> {
        act_ctx
            .register_socket_holder(SocketHolder::new(
                true,
                false,
                From::from(PMQ::new(self.path, libc::O_RDWR, Some(self.on_receive))?),
            ))
            .map(From::from)
    }
}

#[cfg(test)]
mod tests {
    use crate::pmq::{PMQId, PMQ};
    use crate::{Application, ActorioContext, SocketOwner};
    use std::io::Error;

    #[derive(Default)]
    struct PMQApp {
        pmq_in_id: Option<PMQId>,
        pmq_out_id: Option<PMQId>,
        received_count: u8,
        sent_count: u8,
    }

    impl PMQApp {
        fn send_msg(&mut self, act_ctx: &mut ActorioContext<'_, Self>, msg: u8) {
            let pmq_id = self.pmq_out_id.as_ref().unwrap();
            let pmq = act_ctx.get_socket(pmq_id);
            let mut buf = [0u8; 1];
            buf[0] = msg;
            pmq.send(&buf).unwrap();
            self.sent_count += 1;
        }
    }

    impl Application for PMQApp {
        fn start<'a>(&mut self, act_ctx: &mut ActorioContext<'a, Self>) -> Result<(), Error> {
            let mut msg_index = 1;
            let pmq_in_id = PMQ::<PMQApp>::with_path("/actorio-pmq-test")
                .on_receive(move |app: &mut PMQApp, act_ctx, _, buf| {
                    app.received_count += 1;
                    let buf = buf.unwrap();
                    assert_eq!(buf.len(), 1);
                    let msg_num = buf[0];
                    assert_eq!(app.received_count, msg_num);
                    assert_eq!(app.sent_count, msg_num);
                    if msg_num == 5 {
                        act_ctx.close_socket(app.pmq_in_id.take().unwrap());
                        act_ctx.close_socket(app.pmq_out_id.take().unwrap());
                        PMQ::<PMQApp>::unlink("/actorio-pmq-test").unwrap();
                        act_ctx.stop();
                    } else {
                        msg_index += 1;
                        app.send_msg(act_ctx, msg_index);
                    }
                })
                .open_readonly(act_ctx)?;
            let pmq_out_id =
                PMQ::<PMQApp>::with_path("/actorio-pmq-test").open_writeonly(act_ctx)?;
            self.pmq_in_id = Some(pmq_in_id);
            self.pmq_out_id = Some(pmq_out_id);
            self.send_msg(act_ctx, 1);
            Ok(())
        }
    }

    #[test]
    fn test() {
        let app: PMQApp = Default::default();
        let app = ActorioContext::run(app).unwrap();
        assert_eq!(app.received_count, 5);
        assert_eq!(app.sent_count, 5);
    }
}
