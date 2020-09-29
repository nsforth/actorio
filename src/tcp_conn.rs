use crate::{AsSocketId, MaybeSocketOwner, ActorioContext, SocketHolder, SocketId};
use mio::event::Source;
use mio::net::TcpStream;
use mio::{Interest, Registry, Token};
use std::io::{Error, Read, Write};
use std::net::SocketAddr;

type TCPEventHandler<'a, A> = Box<dyn FnMut(&mut A, &mut ActorioContext<'a, A>, &TCPConnId) + 'a>;

#[derive(Hash, PartialEq, Eq, Debug)]
pub struct TCPConnId(SocketId);

impl AsSocketId for TCPConnId {
    fn as_socket_id(&self) -> &SocketId {
        &self.0
    }
}

impl From<SocketId> for TCPConnId {
    fn from(socket_id: SocketId) -> Self {
        TCPConnId(socket_id)
    }
}

pub struct TCPConn<'a, A> {
    tcp_stream: TcpStream,
    on_readable: Option<TCPEventHandler<'a, A>>,
    on_writeable: Option<TCPEventHandler<'a, A>>,
}

impl<'a, A> TCPConn<'a, A> {
    pub fn connect(addr: SocketAddr) -> Result<TCPConnInit, Error> {
        TCPConnInit::connect(addr)
    }

    pub(crate) fn process_read(
        act_ctx: &mut ActorioContext<'a, A>,
        application: &mut A,
        tcp_conn_id: TCPConnId,
    ) {
        if let Some(tcp_conn) = act_ctx.try_get_socket(&tcp_conn_id) {
            // Предполагается, что в None его могут перевести только здесь через take.
            let mut on_readable = tcp_conn.on_readable.take().unwrap();
            on_readable(application, act_ctx, &tcp_conn_id);
            if let Some(tcp_conn) = act_ctx.try_get_socket(&tcp_conn_id) {
                // А если приложение не установило во время вызова новый обработчик, то вернем на место старый
                if tcp_conn.on_readable.is_none() {
                    tcp_conn.on_readable = Some(on_readable);
                }
            };
        };
    }

    pub(crate) fn process_write(
        act_ctx: &mut ActorioContext<'a, A>,
        application: &mut A,
        tcp_conn_id: TCPConnId,
    ) {
        if let Some(tcp_conn) = act_ctx.try_get_socket(&tcp_conn_id) {
            // Предполагается, что в None его могут перевести только здесь через take.
            let mut on_writeable = tcp_conn.on_writeable.take().unwrap();
            on_writeable(application, act_ctx, &tcp_conn_id);
            if let Some(tcp_conn) = act_ctx.try_get_socket(&tcp_conn_id) {
                // А если приложение не установило во время вызова новый обработчик, то вернем на место старый
                if tcp_conn.on_writeable.is_none() {
                    tcp_conn.on_writeable = Some(on_writeable);
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

impl<'a, A> TCPConn<'a, A> {
    pub fn local_addr(&self) -> Result<SocketAddr, Error> {
        self.tcp_stream.local_addr()
    }

    pub fn peer_addr(&self) -> Result<SocketAddr, Error> {
        self.tcp_stream.peer_addr()
    }
}

impl<'a, A> Source for TCPConn<'a, A> {
    fn register(
        &mut self,
        registry: &Registry,
        token: Token,
        interests: Interest,
    ) -> Result<(), Error> {
        self.tcp_stream.register(registry, token, interests)
    }

    fn reregister(
        &mut self,
        registry: &Registry,
        token: Token,
        interests: Interest,
    ) -> Result<(), Error> {
        self.tcp_stream.reregister(registry, token, interests)
    }

    fn deregister(&mut self, registry: &Registry) -> Result<(), Error> {
        self.tcp_stream.deregister(registry)
    }
}

impl<'a, A> Read for TCPConn<'a, A> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, Error> {
        self.tcp_stream.read(buf)
    }
}

impl<'a, A> Write for TCPConn<'a, A> {
    fn write(&mut self, buf: &[u8]) -> Result<usize, Error> {
        self.tcp_stream.write(buf)
    }

    fn flush(&mut self) -> Result<(), Error> {
        self.tcp_stream.flush()
    }
}

pub struct TCPConnInit(TcpStream);

impl TCPConnInit {
    fn connect(addr: SocketAddr) -> Result<Self, Error> {
        Ok(TCPConnInit(TcpStream::connect(addr)?))
    }

    pub(crate) fn new(tcp_stream: TcpStream) -> Self {
        TCPConnInit(tcp_stream)
    }

    pub fn on_readable<'a, A>(
        self,
        on_readable: impl FnMut(&mut A, &mut ActorioContext<A>, &TCPConnId) + 'a,
    ) -> TCPConnInitWithReadHandler<'a, A> {
        TCPConnInitWithReadHandler {
            tcp_stream: self.0,
            on_readable: Box::new(on_readable),
        }
    }

    pub fn on_writeable<'a, A>(
        self,
        on_writeable: impl FnMut(&mut A, &mut ActorioContext<A>, &TCPConnId) + 'a,
    ) -> TCPConnInitWithWriteHandler<'a, A> {
        TCPConnInitWithWriteHandler {
            tcp_stream: self.0,
            on_writeable: Box::new(on_writeable),
        }
    }
}

pub struct TCPConnInitWithReadHandler<'a, A> {
    tcp_stream: TcpStream,
    on_readable: TCPEventHandler<'a, A>,
}

impl<'a, A> TCPConnInitWithReadHandler<'a, A> {
    pub fn on_writeable(
        self,
        on_writeable: impl FnMut(&mut A, &mut ActorioContext<A>, &TCPConnId) + 'a,
    ) -> TCPConnInitFinal<'a, A> {
        TCPConnInitFinal {
            tcp_stream: self.tcp_stream,
            on_readable: Some(self.on_readable),
            on_writeable: Some(Box::new(on_writeable)),
        }
    }

    pub fn register(self, act_ctx: &mut ActorioContext<'a, A>) -> Result<TCPConnId, Error> {
        let tcp_conn_init_final = TCPConnInitFinal {
            tcp_stream: self.tcp_stream,
            on_readable: Some(self.on_readable),
            on_writeable: None,
        };
        tcp_conn_init_final.register(act_ctx)
    }
}

pub struct TCPConnInitWithWriteHandler<'a, A> {
    tcp_stream: TcpStream,
    on_writeable: TCPEventHandler<'a, A>,
}

impl<'a, A> TCPConnInitWithWriteHandler<'a, A> {
    pub fn on_readable(
        self,
        on_readable: impl FnMut(&mut A, &mut ActorioContext<A>, &TCPConnId) + 'a,
    ) -> TCPConnInitFinal<'a, A> {
        TCPConnInitFinal {
            tcp_stream: self.tcp_stream,
            on_readable: Some(Box::new(on_readable)),
            on_writeable: Some(self.on_writeable),
        }
    }

    pub fn register(self, act_ctx: &mut ActorioContext<'a, A>) -> Result<TCPConnId, Error> {
        let tcp_conn_init_final = TCPConnInitFinal {
            tcp_stream: self.tcp_stream,
            on_readable: None,
            on_writeable: Some(self.on_writeable),
        };
        tcp_conn_init_final.register(act_ctx)
    }
}

pub struct TCPConnInitFinal<'a, A> {
    tcp_stream: TcpStream,
    on_readable: Option<TCPEventHandler<'a, A>>,
    on_writeable: Option<TCPEventHandler<'a, A>>,
}

impl<'a, A> TCPConnInitFinal<'a, A> {
    pub fn register(self, act_ctx: &mut ActorioContext<'a, A>) -> Result<TCPConnId, Error> {
        act_ctx
            .register_socket_holder(SocketHolder::new(
                self.on_readable.is_some(),
                self.on_writeable.is_some(),
                From::from(TCPConn {
                    tcp_stream: self.tcp_stream,
                    on_readable: self.on_readable,
                    on_writeable: self.on_writeable,
                }),
            ))
            .map(From::from)
    }
}
