pub mod custom_fd;
pub mod hp_timers;
pub mod pmq;
pub mod tcp_conn;
pub mod tcp_srv;
pub mod timers;
pub mod udp_srv;
use crate::custom_fd::{CustomFd, CustomFdId};
use crate::hp_timers::HPTimer;
use crate::hp_timers::HPTimerId;
use crate::pmq::{PMQId, PMQ};
use crate::tcp_conn::{TCPConn, TCPConnId};
use crate::tcp_srv::{TCPSrv, TCPSrvId};
use crate::timers::{TimerId, Timers};
use crate::udp_srv::{UDPSrv, UDPSrvId};
use mio::event::{Event, Source};
use mio::{Events, Interest, Poll, Registry, Token};
use slab::Slab;
use std::fmt::{Debug, Display, Formatter};
use std::io::{Error, ErrorKind};
use std::time::Duration;

const EVENTS_CAPACITY: usize = 1024;
pub(crate) const BUF_SIZE: usize = 65536;

pub struct ActorioContext<'a, A> {
    poll: Poll,
    sockets: Slab<SocketHolder<'a, A>>,
    timers: Timers<'a, A, Self>,
    deferred_operations: Vec<usize>,
    closing_sockets: Vec<usize>,
    stop: bool,
}

impl<'a, A: Application> ActorioContext<'a, A> {
    pub fn run(mut application: A) -> Result<A, Error> {
        let mut sc: ActorioContext<A> = ActorioContext {
            poll: Poll::new()?,
            sockets: Slab::with_capacity(EVENTS_CAPACITY),
            timers: Timers::new(),
            deferred_operations: Vec::new(),
            closing_sockets: Vec::new(),
            stop: false,
        };
        application.start(&mut sc)?;
        // Временный буфер для чтения данных из сокетов
        let mut buf = vec![0; BUF_SIZE];
        let mut events = Events::with_capacity(EVENTS_CAPACITY);
        loop {
            sc.poll.poll(&mut events, sc.timers.delay_to_next_timer())?;
            // Обработка таймеров
            Timers::process_timers(&mut application, &mut sc, |sc| &mut sc.timers);
            // Обработка актуальных событий по открытым сокетам
            for event in events.iter() {
                if event.is_readable() {
                    sc.process_read_events(
                        &mut application,
                        event,
                        From::from(event.token()),
                        &mut buf,
                    );
                }
                if event.is_writable() {
                    sc.process_write_events(&mut application, event, From::from(event.token()));
                }
            }
            // TODO Обработка отложенных операций
            if !sc.deferred_operations.is_empty() {
                for _id in sc.deferred_operations.drain(..) {}
            }
            // TODO Обработка отложенных на завершение сокетов, помимо просто удаления и закрытия
            if !sc.closing_sockets.is_empty() {
                for id in sc.closing_sockets.drain(..) {
                    sc.sockets.remove(id);
                }
            }
            if sc.stop {
                break Ok(application);
            }
        }
    }
}

impl<'a, A> ActorioContext<'a, A> {
    pub fn timer_set_oneshot(
        &mut self,
        delay: Duration,
        func: impl FnOnce(&mut A, &mut Self, &TimerId) -> TimerId + 'a,
    ) -> TimerId {
        self.timers.set_oneshot(delay, func)
    }

    pub fn timer_set_absolute(
        &mut self,
        deadline: Duration,
        func: impl FnOnce(&mut A, &mut Self, &TimerId) -> TimerId + 'a,
    ) -> TimerId {
        self.timers.set_absolute(deadline, func)
    }

    pub fn timer_set_periodic(
        &mut self,
        interval: Duration,
        func: impl FnMut(&mut A, &mut Self, &TimerId) + 'a,
    ) -> TimerId {
        self.timers.set_periodic(interval, func)
    }

    pub fn timer_cancel(&mut self, timer_id: TimerId) {
        self.timers.cancel(timer_id)
    }

    pub fn hptimer_set_oneshot(
        &mut self,
        delay: Duration,
        func: impl FnOnce(&mut A, &mut Self, &HPTimerId) -> HPTimerId + 'a,
    ) -> HPTimerId {
        let hptimer_id = HPTimer::new(self).expect("Error on HPTimer oneshot arming");
        let hp_timer = self.try_get_socket(&hptimer_id).unwrap();
        hp_timer
            .set_oneshot(delay, func)
            .expect("Error on HPTimer oneshot arming");
        hptimer_id
    }

    pub fn hptimer_set_absolute(
        &mut self,
        deadline: Duration,
        func: impl FnOnce(&mut A, &mut Self, &HPTimerId) -> HPTimerId + 'a,
    ) -> HPTimerId {
        let hptimer_id = HPTimer::new(self).expect("Error on HPTimer absolute arming");
        let hp_timer = self.try_get_socket(&hptimer_id).unwrap();
        hp_timer
            .set_absolute(deadline, func)
            .expect("Error on HPTimer absolute arming");
        hptimer_id
    }

    pub fn hptimer_set_periodic(
        &mut self,
        interval: Duration,
        func: impl FnMut(&mut A, &mut Self, &HPTimerId) + 'a,
    ) -> HPTimerId {
        let hptimer_id = HPTimer::new(self).expect("Error on HPTimer periodic arming");
        let hp_timer = self.try_get_socket(&hptimer_id).unwrap();
        hp_timer
            .set_periodic(interval, func)
            .expect("Error on HPTimer periodic arming");
        hptimer_id
    }

    pub fn hptimer_cancel(&mut self, hptimer_id: HPTimerId) {
        let hp_timer = self.try_get_socket(&hptimer_id).unwrap();
        hp_timer.cancel().expect("Error on HPTimer cancel");
        self.close_socket_by_id(hptimer_id.as_socket_id());
    }

    pub fn stop(&mut self) {
        self.stop = true;
    }

    pub fn get_system_time(&self) -> Duration {
        timers::get_system_time()
    }

    fn register_socket_holder(&mut self, mut sh: SocketHolder<'a, A>) -> Result<SocketId, Error> {
        let slab_entry = self.sockets.vacant_entry();
        let token = Token(slab_entry.key());
        if let Some(interest) = SocketHolder::<A>::get_interest(sh.want_read, sh.want_write) {
            let registry = self.poll.registry();
            sh.pollable_socket.register(registry, token, interest)?;
        }
        let socket_id = SocketId(slab_entry.key());
        slab_entry.insert(sh);
        Ok(socket_id)
    }

    fn get_socket_holder(&mut self, socket_id: &SocketId) -> Option<&mut SocketHolder<'a, A>> {
        match self.sockets.get_mut(socket_id.0) {
            Some(sh) if !sh.closing => Some(sh),
            _ => None,
        }
    }

    fn change_registration(
        &mut self,
        socket_id: &SocketId,
        will_read: Option<bool>,
        will_write: Option<bool>,
    ) -> Result<(), Error> {
        // TODO Исправить с проверкой, если сокет уже закрыт (closing = true)
        let sh = self
            .sockets
            .get_mut(socket_id.0)
            .ok_or_else(|| Error::from(ErrorKind::NotFound))?;
        sh.change_registration(self.poll.registry(), socket_id, will_read, will_write)
    }

    fn process_read_events(
        &mut self,
        application: &mut A,
        event: &Event,
        socket_id: SocketId,
        buf: &mut Vec<u8>,
    ) {
        let pollable_socket = self
            .get_socket_holder(&socket_id)
            .map(|sh| &sh.pollable_socket);
        match pollable_socket {
            Some(PollableSocket::HPTimer(_)) => {
                HPTimer::process_hptimer(self, application, From::from(socket_id))
            }
            Some(PollableSocket::UDPSrv(_)) => {
                UDPSrv::process_receive(self, application, From::from(socket_id), buf)
            }
            Some(PollableSocket::TCPSrv(_)) => {
                TCPSrv::accept(self, application, From::from(socket_id))
            }
            Some(PollableSocket::TCPConn(_)) => {
                TCPConn::process_read(self, application, From::from(socket_id))
            }
            Some(PollableSocket::PMQ(_)) => {
                PMQ::process_receive(self, application, From::from(socket_id), buf)
            }
            Some(PollableSocket::CustomFd(_)) => {
                CustomFd::process_read(self, application, From::from(socket_id))
            }
            None => log::trace!(
                "Socket #{} is in closing state, read event {:?} is ignored",
                socket_id,
                event
            ),
        }
    }

    fn process_write_events(&mut self, application: &mut A, event: &Event, socket_id: SocketId) {
        let pollable_socket = self
            .get_socket_holder(&socket_id)
            .map(|sh| &sh.pollable_socket);
        match pollable_socket {
            Some(PollableSocket::UDPSrv(_)) => {
                // TODO Запись исходящего сообщения UDP сервера
            }
            Some(PollableSocket::TCPConn(_)) => {
                TCPConn::process_write(self, application, From::from(socket_id))
            }
            Some(PollableSocket::CustomFd(_)) => {
                CustomFd::process_write(self, application, From::from(socket_id))
            }
            Some(_) => log::error!(
                "Unexpected write event on socket #{}: {:?}",
                socket_id,
                event
            ),
            None => log::trace!(
                "Socket #{} is in closing state, write event {:?} is ignored",
                socket_id,
                event
            ),
        }
    }

    fn close_socket_by_id(&mut self, socket_id: &SocketId) {
        let sh = self
            .sockets
            .get_mut(socket_id.0)
            .expect("Sockets closing should not fail!");
        assert!(!sh.closing, "Socket already closed!");
        sh.closing = true;
        self.closing_sockets.push(socket_id.0);
    }
}

pub trait Application {
    fn start<'a>(&mut self, act_ctx: &mut ActorioContext<'a, Self>) -> Result<(), Error>
    where
        Self: Sized;
}

pub(crate) trait MaybeSocketOwner<'a, S, I: AsSocketId> {
    fn try_get_socket(&mut self, id: &I) -> Option<&mut S>;
}

pub(crate) trait AsSocketId {
    fn as_socket_id(&self) -> &SocketId;
}

pub trait SocketOwner<'a, S, I> {
    fn get_socket(&mut self, id: &I) -> &mut S;
    fn close_socket(&mut self, id: I);
}

impl<'a, A> MaybeSocketOwner<'a, HPTimer<'a, A>, HPTimerId> for ActorioContext<'a, A> {
    fn try_get_socket(&mut self, hptimer_id: &HPTimerId) -> Option<&mut HPTimer<'a, A>> {
        self.get_socket_holder(hptimer_id.as_socket_id())
            .map(|sh| Into::into(&mut sh.pollable_socket))
            .flatten()
    }
}

impl<'a, A> MaybeSocketOwner<'a, TCPSrv<'a, A>, TCPSrvId> for ActorioContext<'a, A> {
    fn try_get_socket(&mut self, tcpsrv_id: &TCPSrvId) -> Option<&mut TCPSrv<'a, A>> {
        self.get_socket_holder(tcpsrv_id.as_socket_id())
            .map(|sh| Into::into(&mut sh.pollable_socket))
            .flatten()
    }
}

impl<'a, A> SocketOwner<'a, TCPSrv<'a, A>, TCPSrvId> for ActorioContext<'a, A> {
    fn get_socket(&mut self, tcpsrv_id: &TCPSrvId) -> &mut TCPSrv<'a, A> {
        self.try_get_socket(tcpsrv_id).unwrap()
    }

    fn close_socket(&mut self, tcpsrv_id: TCPSrvId) {
        self.close_socket_by_id(tcpsrv_id.as_socket_id());
    }
}

impl<'a, A> MaybeSocketOwner<'a, TCPConn<'a, A>, TCPConnId> for ActorioContext<'a, A> {
    fn try_get_socket(&mut self, tcpconn_id: &TCPConnId) -> Option<&mut TCPConn<'a, A>> {
        self.get_socket_holder(tcpconn_id.as_socket_id())
            .map(|sh| Into::into(&mut sh.pollable_socket))
            .flatten()
    }
}

impl<'a, A> SocketOwner<'a, TCPConn<'a, A>, TCPConnId> for ActorioContext<'a, A> {
    fn get_socket(&mut self, tcpconn_id: &TCPConnId) -> &mut TCPConn<'a, A> {
        self.try_get_socket(tcpconn_id).unwrap()
    }

    fn close_socket(&mut self, tcpconn_id: TCPConnId) {
        self.close_socket_by_id(tcpconn_id.as_socket_id())
    }
}

impl<'a, A> MaybeSocketOwner<'a, PMQ<'a, A>, PMQId> for ActorioContext<'a, A> {
    fn try_get_socket(&mut self, pmq_id: &PMQId) -> Option<&mut PMQ<'a, A>> {
        self.get_socket_holder(pmq_id.as_socket_id())
            .map(|sh| Into::into(&mut sh.pollable_socket))
            .flatten()
    }
}

impl<'a, A> SocketOwner<'a, PMQ<'a, A>, PMQId> for ActorioContext<'a, A> {
    fn get_socket(&mut self, pmq_id: &PMQId) -> &mut PMQ<'a, A> {
        self.try_get_socket(pmq_id).unwrap()
    }

    fn close_socket(&mut self, pmq_id: PMQId) {
        self.close_socket_by_id(pmq_id.as_socket_id())
    }
}

impl<'a, A> MaybeSocketOwner<'a, UDPSrv<'a, A>, UDPSrvId> for ActorioContext<'a, A> {
    fn try_get_socket(&mut self, udpsrv_id: &UDPSrvId) -> Option<&mut UDPSrv<'a, A>> {
        self.get_socket_holder(udpsrv_id.as_socket_id())
            .map(|sh| Into::into(&mut sh.pollable_socket))
            .flatten()
    }
}

impl<'a, A> SocketOwner<'a, UDPSrv<'a, A>, UDPSrvId> for ActorioContext<'a, A> {
    fn get_socket(&mut self, udpsrv_id: &UDPSrvId) -> &mut UDPSrv<'a, A> {
        self.try_get_socket(udpsrv_id).unwrap()
    }

    fn close_socket(&mut self, udpsrv_id: UDPSrvId) {
        self.close_socket_by_id(udpsrv_id.as_socket_id())
    }
}

impl<'a, A> MaybeSocketOwner<'a, CustomFd<'a, A>, CustomFdId> for ActorioContext<'a, A> {
    fn try_get_socket(&mut self, custom_fd_id: &CustomFdId) -> Option<&mut CustomFd<'a, A>> {
        self.get_socket_holder(custom_fd_id.as_socket_id())
            .map(|sh| Into::into(&mut sh.pollable_socket))
            .flatten()
    }
}

impl<'a, A> SocketOwner<'a, CustomFd<'a, A>, CustomFdId> for ActorioContext<'a, A> {
    fn get_socket(&mut self, custom_fd_id: &CustomFdId) -> &mut CustomFd<'a, A> {
        self.try_get_socket(custom_fd_id).unwrap()
    }

    fn close_socket(&mut self, custom_fd_id: CustomFdId) {
        self.close_socket_by_id(custom_fd_id.as_socket_id())
    }
}

pub trait SocketController<I> {
    fn resume_read(&mut self, socket_id: &I) -> Result<(), Error>;

    fn resume_write(&mut self, socket_id: &I) -> Result<(), Error>;

    fn resume_all(&mut self, socket_id: &I) -> Result<(), Error>;

    fn suspend_read(&mut self, socket_id: &I) -> Result<(), Error>;

    fn suspend_write(&mut self, socket_id: &I) -> Result<(), Error>;

    fn suspend_all(&mut self, socket_id: &I) -> Result<(), Error>;
}

impl<'a, A> SocketController<TCPConnId> for ActorioContext<'a, A> {
    fn resume_read(&mut self, socket_id: &TCPConnId) -> Result<(), Error> {
        assert!(
            self.get_socket(socket_id).has_read_handler(),
            "No read handler installed on socket {:?}",
            socket_id
        );
        self.change_registration(socket_id.as_socket_id(), Some(true), None)
    }

    fn resume_write(&mut self, socket_id: &TCPConnId) -> Result<(), Error> {
        assert!(
            self.get_socket(socket_id).has_write_handler(),
            "No write handler installed on socket {:?}",
            socket_id
        );
        self.change_registration(socket_id.as_socket_id(), None, Some(true))
    }

    fn resume_all(&mut self, socket_id: &TCPConnId) -> Result<(), Error> {
        let socket = self.get_socket(socket_id);
        assert!(
            socket.has_read_handler(),
            "No read handler installed on socket {:?}",
            socket_id
        );
        assert!(
            socket.has_write_handler(),
            "No write handler installed on socket {:?}",
            socket_id
        );
        self.change_registration(socket_id.as_socket_id(), Some(true), Some(true))
    }

    fn suspend_read(&mut self, socket_id: &TCPConnId) -> Result<(), Error> {
        self.change_registration(socket_id.as_socket_id(), Some(false), None)
    }

    fn suspend_write(&mut self, socket_id: &TCPConnId) -> Result<(), Error> {
        self.change_registration(socket_id.as_socket_id(), None, Some(false))
    }

    fn suspend_all(&mut self, socket_id: &TCPConnId) -> Result<(), Error> {
        self.change_registration(socket_id.as_socket_id(), Some(false), Some(false))
    }
}

impl<'a, A> SocketController<CustomFdId> for ActorioContext<'a, A> {
    fn resume_read(&mut self, socket_id: &CustomFdId) -> Result<(), Error> {
        assert!(
            self.get_socket(socket_id).has_read_handler(),
            "No read handler installed on socket {:?}",
            socket_id
        );
        self.change_registration(socket_id.as_socket_id(), Some(true), None)
    }

    fn resume_write(&mut self, socket_id: &CustomFdId) -> Result<(), Error> {
        assert!(
            self.get_socket(socket_id).has_write_handler(),
            "No write handler installed on socket {:?}",
            socket_id
        );
        self.change_registration(socket_id.as_socket_id(), None, Some(true))
    }

    fn resume_all(&mut self, socket_id: &CustomFdId) -> Result<(), Error> {
        let socket = self.get_socket(socket_id);
        assert!(
            socket.has_read_handler(),
            "No read handler installed on socket {:?}",
            socket_id
        );
        assert!(
            socket.has_write_handler(),
            "No write handler installed on socket {:?}",
            socket_id
        );
        self.change_registration(socket_id.as_socket_id(), Some(true), Some(true))
    }

    fn suspend_read(&mut self, socket_id: &CustomFdId) -> Result<(), Error> {
        self.change_registration(socket_id.as_socket_id(), Some(false), None)
    }

    fn suspend_write(&mut self, socket_id: &CustomFdId) -> Result<(), Error> {
        self.change_registration(socket_id.as_socket_id(), None, Some(false))
    }

    fn suspend_all(&mut self, socket_id: &CustomFdId) -> Result<(), Error> {
        self.change_registration(socket_id.as_socket_id(), Some(false), Some(false))
    }
}

#[derive(Hash, PartialEq, Eq, Debug)]
struct SocketId(usize);

impl Display for SocketId {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", self.0)
    }
}

impl Into<Token> for &SocketId {
    fn into(self) -> Token {
        Token(self.0)
    }
}

impl From<Token> for SocketId {
    fn from(token: Token) -> Self {
        SocketId(token.0)
    }
}

struct SocketHolder<'a, A> {
    want_read: bool,
    want_write: bool,
    closing: bool,
    pollable_socket: PollableSocket<'a, A>,
}

impl<'a, A> SocketHolder<'a, A> {
    fn new(want_read: bool, want_write: bool, pollable_socket: PollableSocket<'a, A>) -> Self {
        SocketHolder {
            want_read,
            want_write,
            closing: false,
            pollable_socket,
        }
    }

    fn get_interest(want_read: bool, want_write: bool) -> Option<Interest> {
        match (want_read, want_write) {
            (true, true) => Some(Interest::READABLE | Interest::WRITABLE),
            (true, false) => Some(Interest::READABLE),
            (false, true) => Some(Interest::WRITABLE),
            (false, false) => None,
        }
    }

    fn change_registration(
        &mut self,
        registry: &Registry,
        socket_id: &SocketId,
        will_read: Option<bool>,
        will_write: Option<bool>,
    ) -> Result<(), Error> {
        let will_read = will_read.unwrap_or(self.want_read);
        let will_write = will_write.unwrap_or(self.want_write);
        let new_interest = SocketHolder::<A>::get_interest(will_read, will_write);
        let old_interest = SocketHolder::<A>::get_interest(self.want_read, self.want_write);
        match (new_interest, old_interest) {
            (Some(ni), Some(oi)) if ni != oi => {
                registry.reregister(&mut self.pollable_socket, socket_id.into(), ni)?
            }
            (Some(ni), None) => {
                registry.register(&mut self.pollable_socket, socket_id.into(), ni)?
            }
            (None, Some(_)) => registry.deregister(&mut self.pollable_socket)?,
            (_, current) => {
                log::trace!(
                    "Socket {} registration has not changed, stay same {:?}",
                    socket_id,
                    current
                );
                return Ok(());
            }
        };
        self.want_read = will_read;
        self.want_write = will_write;
        Ok(())
    }
}

enum PollableSocket<'a, A> {
    HPTimer(HPTimer<'a, A>),
    UDPSrv(UDPSrv<'a, A>),
    TCPSrv(TCPSrv<'a, A>),
    TCPConn(TCPConn<'a, A>),
    PMQ(PMQ<'a, A>),
    CustomFd(CustomFd<'a, A>),
}

impl<'a, 'b, A> Into<Option<&'b mut HPTimer<'a, A>>> for &'b mut PollableSocket<'a, A> {
    fn into(self) -> Option<&'b mut HPTimer<'a, A>> {
        match self {
            PollableSocket::HPTimer(hptimer) => Some(hptimer),
            _ => None,
        }
    }
}

impl<'a, A> From<HPTimer<'a, A>> for PollableSocket<'a, A> {
    fn from(hptimer: HPTimer<'a, A>) -> Self {
        PollableSocket::HPTimer(hptimer)
    }
}

impl<'a, 'b, A> Into<Option<&'b mut TCPSrv<'a, A>>> for &'b mut PollableSocket<'a, A> {
    fn into(self) -> Option<&'b mut TCPSrv<'a, A>> {
        match self {
            PollableSocket::TCPSrv(tcp_srv) => Some(tcp_srv),
            _ => None,
        }
    }
}

impl<'a, A> From<TCPSrv<'a, A>> for PollableSocket<'a, A> {
    fn from(tcp_srv: TCPSrv<'a, A>) -> Self {
        PollableSocket::TCPSrv(tcp_srv)
    }
}

impl<'a, 'b, A> Into<Option<&'b mut TCPConn<'a, A>>> for &'b mut PollableSocket<'a, A> {
    fn into(self) -> Option<&'b mut TCPConn<'a, A>> {
        match self {
            PollableSocket::TCPConn(tcp_conn) => Some(tcp_conn),
            _ => None,
        }
    }
}

impl<'a, A> From<TCPConn<'a, A>> for PollableSocket<'a, A> {
    fn from(tcp_conn: TCPConn<'a, A>) -> Self {
        PollableSocket::TCPConn(tcp_conn)
    }
}

impl<'a, 'b, A> Into<Option<&'b mut PMQ<'a, A>>> for &'b mut PollableSocket<'a, A> {
    fn into(self) -> Option<&'b mut PMQ<'a, A>> {
        match self {
            PollableSocket::PMQ(pmq) => Some(pmq),
            _ => None,
        }
    }
}

impl<'a, A> From<PMQ<'a, A>> for PollableSocket<'a, A> {
    fn from(pmq: PMQ<'a, A>) -> Self {
        PollableSocket::PMQ(pmq)
    }
}

impl<'a, 'b, A> Into<Option<&'b mut UDPSrv<'a, A>>> for &'b mut PollableSocket<'a, A> {
    fn into(self) -> Option<&'b mut UDPSrv<'a, A>> {
        match self {
            PollableSocket::UDPSrv(udpsrv) => Some(udpsrv),
            _ => None,
        }
    }
}

impl<'a, A> From<UDPSrv<'a, A>> for PollableSocket<'a, A> {
    fn from(udpsrv: UDPSrv<'a, A>) -> Self {
        PollableSocket::UDPSrv(udpsrv)
    }
}

impl<'a, 'b, A> Into<Option<&'b mut CustomFd<'a, A>>> for &'b mut PollableSocket<'a, A> {
    fn into(self) -> Option<&'b mut CustomFd<'a, A>> {
        match self {
            PollableSocket::CustomFd(custom_fd) => Some(custom_fd),
            _ => None,
        }
    }
}

impl<'a, A> From<CustomFd<'a, A>> for PollableSocket<'a, A> {
    fn from(custom_fd: CustomFd<'a, A>) -> Self {
        PollableSocket::CustomFd(custom_fd)
    }
}

impl<'a, A> Source for PollableSocket<'a, A> {
    fn register(
        &mut self,
        registry: &Registry,
        token: Token,
        interests: Interest,
    ) -> Result<(), Error> {
        match self {
            PollableSocket::HPTimer(hptimer) => hptimer.register(registry, token, interests),
            PollableSocket::UDPSrv(udp_srv) => udp_srv.register(registry, token, interests),
            PollableSocket::TCPSrv(tcp_srv) => tcp_srv.register(registry, token, interests),
            PollableSocket::TCPConn(tcp_conn) => tcp_conn.register(registry, token, interests),
            PollableSocket::PMQ(pmq) => pmq.register(registry, token, interests),
            PollableSocket::CustomFd(custom_fd) => custom_fd.register(registry, token, interests),
        }
    }

    fn reregister(
        &mut self,
        registry: &Registry,
        token: Token,
        interests: Interest,
    ) -> Result<(), Error> {
        match self {
            PollableSocket::HPTimer(_) => panic!("Not implemented for HPTimer"),
            PollableSocket::UDPSrv(udp_srv) => udp_srv.reregister(registry, token, interests),
            PollableSocket::TCPSrv(tcp_srv) => tcp_srv.reregister(registry, token, interests),
            PollableSocket::TCPConn(tcp_conn) => tcp_conn.reregister(registry, token, interests),
            PollableSocket::PMQ(pmq) => pmq.reregister(registry, token, interests),
            PollableSocket::CustomFd(custom_fd) => custom_fd.reregister(registry, token, interests),
        }
    }

    fn deregister(&mut self, registry: &Registry) -> Result<(), Error> {
        match self {
            PollableSocket::HPTimer(hp_timer) => hp_timer.deregister(registry),
            PollableSocket::UDPSrv(udp_srv) => udp_srv.deregister(registry),
            PollableSocket::TCPSrv(tcp_srv) => tcp_srv.deregister(registry),
            PollableSocket::TCPConn(tcp_conn) => tcp_conn.deregister(registry),
            PollableSocket::PMQ(pmq) => pmq.deregister(registry),
            PollableSocket::CustomFd(custom_fd) => custom_fd.deregister(registry),
        }
    }
}
