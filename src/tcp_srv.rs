use crate::tcp_conn::TCPConnInit;
use crate::{AsSocketId, MaybeSocketOwner, ActorioContext, SocketHolder, SocketId};
use mio::event::Source;
use mio::net::TcpListener;
use mio::{Interest, Registry, Token};
use std::io::{Error, ErrorKind};
use std::net::SocketAddr;

type NewConnHandler<'a, A> =
    Box<dyn FnMut(&mut A, &mut ActorioContext<A>, &TCPSrvId, SocketAddr, TCPConnInit) + 'a>;

pub struct TCPSrv<'a, A> {
    tcp_listener: TcpListener,
    on_new_connection: Option<NewConnHandler<'a, A>>,
}

#[derive(Hash, PartialEq, Eq, Debug)]
pub struct TCPSrvId(SocketId);

impl AsSocketId for TCPSrvId {
    fn as_socket_id(&self) -> &SocketId {
        &self.0
    }
}

impl From<SocketId> for TCPSrvId {
    fn from(socket_id: SocketId) -> Self {
        TCPSrvId(socket_id)
    }
}

impl<'a, A> TCPSrv<'a, A> {
    pub fn listen(
        act_ctx: &mut ActorioContext<'a, A>,
        socket_addr: SocketAddr,
        on_new_connection: impl FnMut(&mut A, &mut ActorioContext<A>, &TCPSrvId, SocketAddr, TCPConnInit)
            + 'a,
    ) -> Result<TCPSrvId, Error> {
        let tcp_listener = TcpListener::bind(socket_addr)?;
        act_ctx
            .register_socket_holder(SocketHolder::new(
                true,
                false,
                From::from(TCPSrv {
                    tcp_listener,
                    on_new_connection: Some(Box::new(on_new_connection)),
                }),
            ))
            .map(From::from)
    }

    pub(crate) fn accept(
        act_ctx: &mut ActorioContext<'a, A>,
        application: &mut A,
        tcpsrv_id: TCPSrvId,
    ) {
        while let Some(tcp_srv) = act_ctx.try_get_socket(&tcpsrv_id) {
            match tcp_srv.tcp_listener.accept() {
                Ok((tcp_stream, peer_addr)) => {
                    let tcpconn_init = TCPConnInit::new(tcp_stream);
                    // Предполагается, что в None его могут перевести только здесь через take.
                    let mut on_new_connection = tcp_srv.on_new_connection.take().unwrap();
                    on_new_connection(application, act_ctx, &tcpsrv_id, peer_addr, tcpconn_init);
                    // Если серверное соединение не было закрыто приложением, надо вернуть обратно обработчик
                    if let Some(tcp_srv) = act_ctx.try_get_socket(&tcpsrv_id) {
                        // А если приложение не установило во время вызова новый обработчик, то вернем на место старый
                        if tcp_srv.on_new_connection.is_none() {
                            tcp_srv.on_new_connection = Some(on_new_connection);
                        }
                    }
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => {
                    break;
                }
                Err(e) if e.kind() == ErrorKind::Interrupted => log::trace!(
                    "EINTR on accepting connections from TCP Server {:?}",
                    tcpsrv_id
                ),
                Err(e) => {
                    log::error!(
                        "Error on accepting connections from TCP Server {:?}: {}",
                        tcpsrv_id,
                        e
                    );
                    break;
                }
            };
        }
    }
}

impl<'a, A> Source for TCPSrv<'a, A> {
    fn register(
        &mut self,
        registry: &Registry,
        token: Token,
        interests: Interest,
    ) -> Result<(), Error> {
        self.tcp_listener.register(registry, token, interests)
    }

    fn reregister(
        &mut self,
        registry: &Registry,
        token: Token,
        interests: Interest,
    ) -> Result<(), Error> {
        self.tcp_listener.reregister(registry, token, interests)
    }

    fn deregister(&mut self, registry: &Registry) -> Result<(), Error> {
        self.tcp_listener.deregister(registry)
    }
}
