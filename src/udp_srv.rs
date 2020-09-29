use crate::{AsSocketId, MaybeSocketOwner, ActorioContext, SocketHolder, SocketId};
use mio::event::Source;
use mio::net::UdpSocket;
use mio::{Interest, Registry, Token};
use std::io::{Error, ErrorKind};
use std::net::SocketAddr;

type UDPSrvReceiveHandler<'a, A> = Box<
    dyn FnMut(&mut A, &mut ActorioContext<'a, A>, &UDPSrvId, Result<(&[u8], &SocketAddr), Error>)
        + 'a,
>;

#[derive(Hash, PartialEq, Eq, Debug)]
pub struct UDPSrvId(SocketId);

impl AsSocketId for UDPSrvId {
    fn as_socket_id(&self) -> &SocketId {
        &self.0
    }
}

impl From<SocketId> for UDPSrvId {
    fn from(socket_id: SocketId) -> Self {
        UDPSrvId(socket_id)
    }
}

pub struct UDPSrv<'a, A> {
    udp_socket: UdpSocket,
    on_receive: Option<UDPSrvReceiveHandler<'a, A>>,
}

impl<'a, A> UDPSrv<'a, A> {
    fn new(
        listen_addr: SocketAddr,
        on_receive: Option<UDPSrvReceiveHandler<'a, A>>,
    ) -> Result<Self, Error> {
        Ok(UDPSrv {
            udp_socket: UdpSocket::bind(listen_addr)?,
            on_receive,
        })
    }

    pub fn send_to(&self, peer: SocketAddr, buf: &[u8]) -> Result<usize, Error> {
        self.udp_socket.send_to(buf, peer)
    }

    pub fn local_addr(&self) -> Result<SocketAddr, Error> {
        self.udp_socket.local_addr()
    }

    pub(crate) fn process_receive(
        act_ctx: &mut ActorioContext<'a, A>,
        application: &mut A,
        udpsrv_id: UDPSrvId,
        buf: &mut Vec<u8>,
    ) {
        while let Some(udpsrv) = act_ctx.try_get_socket(&udpsrv_id) {
            match udpsrv.udp_socket.recv_from(buf) {
                Ok((count, peer)) => {
                    let mut on_receive = udpsrv.on_receive.take().unwrap();
                    on_receive(
                        application,
                        act_ctx,
                        &udpsrv_id,
                        Ok((&buf[0..count], &peer)),
                    );
                    if let Some(udpsrv) = act_ctx.try_get_socket(&udpsrv_id) {
                        udpsrv.on_receive = Some(on_receive);
                    } else {
                        break;
                    }
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                Err(e) if e.kind() == ErrorKind::Interrupted => continue,
                Err(e) => {
                    let mut on_receive = udpsrv.on_receive.take().unwrap();
                    on_receive(application, act_ctx, &udpsrv_id, Err(e));
                    if let Some(udpsrv) = act_ctx.try_get_socket(&udpsrv_id) {
                        udpsrv.on_receive = Some(on_receive);
                    } else {
                        break;
                    }
                }
            }
        }
    }
}

impl<'a, A> Source for UDPSrv<'a, A> {
    fn register(
        &mut self,
        registry: &Registry,
        token: Token,
        interests: Interest,
    ) -> Result<(), Error> {
        registry.register(&mut self.udp_socket, token, interests)
    }

    fn reregister(
        &mut self,
        registry: &Registry,
        token: Token,
        interests: Interest,
    ) -> Result<(), Error> {
        registry.reregister(&mut self.udp_socket, token, interests)
    }

    fn deregister(&mut self, registry: &Registry) -> Result<(), Error> {
        registry.deregister(&mut self.udp_socket)
    }
}

pub struct UDPSrvInit;

impl UDPSrvInit {
    pub fn builder() -> Self {
        UDPSrvInit {}
    }

    pub fn on_receive<'a, A>(
        self,
        on_receive: impl FnMut(&mut A, &mut ActorioContext<A>, &UDPSrvId, Result<(&[u8], &SocketAddr), Error>)
            + 'a,
    ) -> UDPSrvInitWithReceiveHandler<'a, A> {
        UDPSrvInitWithReceiveHandler {
            on_receive: Box::new(on_receive),
        }
    }
}

pub struct UDPSrvInitWithReceiveHandler<'a, A> {
    on_receive: UDPSrvReceiveHandler<'a, A>,
}

impl<'a, A> UDPSrvInitWithReceiveHandler<'a, A> {
    pub fn listen(
        self,
        listen_addr: SocketAddr,
        act_ctx: &mut ActorioContext<'a, A>,
    ) -> Result<UDPSrvId, Error> {
        act_ctx
            .register_socket_holder(SocketHolder::new(
                true,
                false,
                From::from(UDPSrv::new(listen_addr, Some(self.on_receive))?),
            ))
            .map(From::from)
    }
}

#[cfg(test)]
mod tests {
    use crate::udp_srv::{UDPSrvId, UDPSrvInit};
    use crate::{Application, ActorioContext, SocketOwner};
    use std::convert::TryInto;
    use std::io::Error;
    use std::net::SocketAddr;
    use std::time::Duration;

    #[derive(Default)]
    struct TestUdpSrvApp {
        udpsrv_id: Option<UDPSrvId>,
        received: usize,
        sent: usize,
    }

    impl TestUdpSrvApp {
        fn on_receive(
            &mut self,
            act_ctx: &mut ActorioContext<Self>,
            usrv_id: &UDPSrvId,
            result: Result<(&[u8], &SocketAddr), Error>,
        ) {
            self.received += 1;
            let (data, peer) = result.unwrap();
            let counter =
                u64::from_be_bytes(data.try_into().expect("UDP message length is not 8 bytes"));
            let counter = counter + 1;
            let udp_srv = act_ctx.get_socket(usrv_id);
            let data = counter.to_be_bytes();
            udp_srv.send_to(*peer, &data).unwrap();
            self.sent += 1;
            if counter == peer.port() as u64 + 10 {
                act_ctx.close_socket(self.udpsrv_id.take().unwrap());
                act_ctx.stop();
            }
        }
    }

    impl Application for TestUdpSrvApp {
        fn start<'a>(&mut self, act_ctx: &mut ActorioContext<'a, Self>) -> Result<(), Error> {
            let udpsrv_id = UDPSrvInit::builder()
                .on_receive(TestUdpSrvApp::on_receive)
                .listen("127.0.0.1:0".parse().unwrap(), act_ctx)?;
            let la = act_ctx.get_socket(&udpsrv_id).local_addr()?;
            self.udpsrv_id = Some(udpsrv_id);
            std::thread::spawn(move || run_test_client(la));
            Ok(())
        }
    }

    fn run_test_client(sa: SocketAddr) {
        let udp_client =
            std::net::UdpSocket::bind("127.0.0.1:0".parse::<SocketAddr>().unwrap()).unwrap();
        udp_client
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();
        udp_client.connect(sa).unwrap();
        let counter_base = udp_client.local_addr().unwrap().port() as u64;
        let mut counter = counter_base;
        loop {
            let data = counter.to_be_bytes();
            udp_client.send(&data).unwrap();
            counter += 1;
            let mut data = [0u8; 8];
            udp_client.recv(&mut data).unwrap();
            let recv_counter = u64::from_be_bytes(data);
            assert_eq!(recv_counter, counter);
            if counter == counter_base + 10 {
                break;
            }
        }
    }

    #[test]
    fn test() {
        let app: TestUdpSrvApp = Default::default();
        let app = ActorioContext::run(app).unwrap();
        assert_eq!(app.received, 10);
        assert_eq!(app.sent, 10);
    }
}
