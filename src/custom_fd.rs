use crate::{AsSocketId, MaybeSocketOwner, ActorioContext, SocketHolder, SocketId};
use mio::event::Source;
use mio::unix::SourceFd;
use mio::{Interest, Registry, Token};
use std::io::Error;
use std::os::unix::io::RawFd;

type CustomFdEventHandler<'a, A> =
    Box<dyn FnMut(&mut A, &mut ActorioContext<'a, A>, &CustomFdId) + 'a>;

#[derive(Hash, PartialEq, Eq, Debug)]
pub struct CustomFdId(SocketId);

impl AsSocketId for CustomFdId {
    fn as_socket_id(&self) -> &SocketId {
        &self.0
    }
}

impl From<SocketId> for CustomFdId {
    fn from(socket_id: SocketId) -> Self {
        CustomFdId(socket_id)
    }
}

pub struct CustomFd<'a, A> {
    custom_fd: RawFd,
    on_readable: Option<CustomFdEventHandler<'a, A>>,
    on_writeable: Option<CustomFdEventHandler<'a, A>>,
}

impl<'a, A> Drop for CustomFd<'a, A> {
    fn drop(&mut self) {
        unsafe { libc::close(self.custom_fd) };
    }
}

impl<'a, A> CustomFd<'a, A> {
    pub fn get_raw_fd(&self) -> RawFd {
        self.custom_fd
    }

    pub fn new(raw_fd: RawFd) -> CustomFdInit {
        CustomFdInit::new::<A>(raw_fd)
    }

    pub(crate) fn process_read(
        act_ctx: &mut ActorioContext<'a, A>,
        application: &mut A,
        custom_fd_id: CustomFdId,
    ) {
        if let Some(custom_fd) = act_ctx.try_get_socket(&custom_fd_id) {
            // Предполагается, что в None его могут перевести только здесь через take.
            let mut on_readable = custom_fd.on_readable.take().unwrap();
            on_readable(application, act_ctx, &custom_fd_id);
            if let Some(custom_fd) = act_ctx.try_get_socket(&custom_fd_id) {
                // А если приложение не установило во время вызова новый обработчик, то вернем на место старый
                if custom_fd.on_readable.is_none() {
                    custom_fd.on_readable = Some(on_readable);
                }
            };
        };
    }

    pub(crate) fn process_write(
        act_ctx: &mut ActorioContext<'a, A>,
        application: &mut A,
        custom_fd_id: CustomFdId,
    ) {
        if let Some(custom_fd) = act_ctx.try_get_socket(&custom_fd_id) {
            // Предполагается, что в None его могут перевести только здесь через take.
            let mut on_writeable = custom_fd.on_writeable.take().unwrap();
            on_writeable(application, act_ctx, &custom_fd_id);
            if let Some(custom_fd) = act_ctx.try_get_socket(&custom_fd_id) {
                // А если приложение не установило во время вызова новый обработчик, то вернем на место старый
                if custom_fd.on_writeable.is_none() {
                    custom_fd.on_writeable = Some(on_writeable);
                }
            };
        };
    }

    pub(crate) fn has_read_handler(&self) -> bool {
        self.on_readable.is_some()
    }

    pub(crate) fn has_write_handler(&self) -> bool {
        self.on_writeable.is_some()
    }
}

impl<'a, A> Source for CustomFd<'a, A> {
    fn register(
        &mut self,
        registry: &Registry,
        token: Token,
        interests: Interest,
    ) -> Result<(), Error> {
        registry.register(&mut SourceFd(&self.custom_fd), token, interests)
    }

    fn reregister(
        &mut self,
        registry: &Registry,
        token: Token,
        interests: Interest,
    ) -> Result<(), Error> {
        registry.reregister(&mut SourceFd(&self.custom_fd), token, interests)
    }

    fn deregister(&mut self, registry: &Registry) -> Result<(), Error> {
        registry.deregister(&mut SourceFd(&self.custom_fd))
    }
}

pub struct CustomFdInit(RawFd);

impl CustomFdInit {
    fn new<A>(raw_fd: RawFd) -> Self {
        CustomFdInit(raw_fd)
    }

    pub fn on_readable<'a, A>(
        self,
        on_readable: impl FnMut(&mut A, &mut ActorioContext<A>, &CustomFdId) + 'a,
    ) -> CustomFdInitWithReadHandler<'a, A> {
        CustomFdInitWithReadHandler {
            custom_fd: self.0,
            on_readable: Box::new(on_readable),
        }
    }

    pub fn on_writeable<'a, A>(
        self,
        on_writeable: impl FnMut(&mut A, &mut ActorioContext<A>, &CustomFdId) + 'a,
    ) -> CustomFdInitWithWriteHandler<'a, A> {
        CustomFdInitWithWriteHandler {
            custom_fd: self.0,
            on_writeable: Box::new(on_writeable),
        }
    }
}

pub struct CustomFdInitWithReadHandler<'a, A> {
    custom_fd: RawFd,
    on_readable: CustomFdEventHandler<'a, A>,
}

impl<'a, A> CustomFdInitWithReadHandler<'a, A> {
    pub fn on_writeable(
        self,
        on_writeable: impl FnMut(&mut A, &mut ActorioContext<A>, &CustomFdId) + 'a,
    ) -> CustomFdInitFinal<'a, A> {
        CustomFdInitFinal {
            custom_fd: self.custom_fd,
            on_readable: Some(self.on_readable),
            on_writeable: Some(Box::new(on_writeable)),
        }
    }

    pub fn register(self, act_ctx: &mut ActorioContext<'a, A>) -> Result<CustomFdId, Error> {
        let custom_fd_init_final = CustomFdInitFinal {
            custom_fd: self.custom_fd,
            on_readable: Some(self.on_readable),
            on_writeable: None,
        };
        custom_fd_init_final.register(act_ctx)
    }
}

pub struct CustomFdInitWithWriteHandler<'a, A> {
    custom_fd: RawFd,
    on_writeable: CustomFdEventHandler<'a, A>,
}

impl<'a, A> CustomFdInitWithWriteHandler<'a, A> {
    pub fn on_readable(
        self,
        on_readable: impl FnMut(&mut A, &mut ActorioContext<A>, &CustomFdId) + 'a,
    ) -> CustomFdInitFinal<'a, A> {
        CustomFdInitFinal {
            custom_fd: self.custom_fd,
            on_readable: Some(Box::new(on_readable)),
            on_writeable: Some(self.on_writeable),
        }
    }

    pub fn register(self, act_ctx: &mut ActorioContext<'a, A>) -> Result<CustomFdId, Error> {
        let custom_fd_init_final = CustomFdInitFinal {
            custom_fd: self.custom_fd,
            on_readable: None,
            on_writeable: Some(self.on_writeable),
        };
        custom_fd_init_final.register(act_ctx)
    }
}

pub struct CustomFdInitFinal<'a, A> {
    custom_fd: RawFd,
    on_readable: Option<CustomFdEventHandler<'a, A>>,
    on_writeable: Option<CustomFdEventHandler<'a, A>>,
}

impl<'a, A> CustomFdInitFinal<'a, A> {
    pub fn register(self, act_ctx: &mut ActorioContext<'a, A>) -> Result<CustomFdId, Error> {
        act_ctx
            .register_socket_holder(SocketHolder::new(
                self.on_readable.is_some(),
                self.on_writeable.is_some(),
                From::from(CustomFd {
                    custom_fd: self.custom_fd,
                    on_readable: self.on_readable,
                    on_writeable: self.on_writeable,
                }),
            ))
            .map(From::from)
    }
}
