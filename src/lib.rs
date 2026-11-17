use crossbeam_channel::{Receiver, Sender, unbounded};
use std::any::Any;
use std::marker::{self, PhantomData};
use std::thread;

trait HasDual {
    type Dual: HasDual;
}

struct Send<A, P>(PhantomData<(A, P)>);

struct Recv<A, P>(PhantomData<(A, P)>);

struct End;

struct Choose<L, R>(PhantomData<(L, R)>);

struct Offer<L, R>(PhantomData<(L, R)>);

impl<A, P: HasDual> HasDual for Send<A, P> {
    type Dual = Recv<A, P::Dual>;
}

impl<A, P: HasDual> HasDual for Recv<A, P> {
    type Dual = Send<A, P::Dual>;
}

impl HasDual for End {
    type Dual = End;
}

impl<L: HasDual, R: HasDual> HasDual for Choose<L, R> {
    type Dual = Offer<L::Dual, R::Dual>;
}

impl<L: HasDual, R: HasDual> HasDual for Offer<L, R> {
    type Dual = Choose<L::Dual, R::Dual>;
}

enum Message {
    Data(Box<dyn Any + marker::Send>),
    Left,
    Right,
}

#[must_use = "session endpoints should be used to complete the protocol"]
struct Channel<P>(Sender<Message>, Receiver<Message>, PhantomData<P>);

pub fn new_session<P: HasDual>() -> (Channel<P>, Channel<P::Dual>) {
    let (s1, r1) = unbounded();
    let (s2, r2) = unbounded();

    let c1 = Channel(s1, r2, PhantomData);
    let c2 = Channel(s2, r1, PhantomData);

    (c1, c2)
}

#[derive(Debug)]
enum Error {
    TypeMismatch,
    UnexpectedMessage,
    Disconnected,
}

impl Channel<End> {
    pub fn close(self) {
        drop(self);
    }
}

impl<P, A> Channel<Send<A, P>> {
    pub fn send(self, v: A) -> Result<Channel<P>, Error>
    where
        A: marker::Send + 'static,
    {
        self.0
            .send(Message::Data(Box::new(v)))
            .map_err(|_| Error::Disconnected)?;
        Ok(Channel(self.0, self.1, PhantomData))
    }
}

impl<P, A> Channel<Recv<A, P>> {
    pub fn recv(self) -> Result<(Channel<P>, A), Error>
    where
        A: marker::Send + 'static,
    {
        let msg = self.1.recv().map_err(|_| Error::Disconnected)?;

        let v = match msg {
            Message::Data(x) => *x.downcast::<A>().map_err(|_| Error::TypeMismatch)?,
            _ => return Err(Error::UnexpectedMessage),
        };

        Ok((Channel(self.0, self.1, PhantomData), v))
    }
}

enum Branch<L, R> {
    Left(Channel<L>),
    Right(Channel<R>),
}

impl<L, R> Channel<Choose<L, R>> {
    pub fn choose_left(self) -> Result<Channel<L>, Error> {
        self.0
            .send(Message::Left)
            .map_err(|_| Error::Disconnected)?;
        Ok(Channel(self.0, self.1, PhantomData))
    }

    pub fn choose_right(self) -> Result<Channel<R>, Error> {
        self.0
            .send(Message::Right)
            .map_err(|_| Error::Disconnected)?;
        Ok(Channel(self.0, self.1, PhantomData))
    }
}

impl<L, R> Channel<Offer<L, R>> {
    pub fn offer(self) -> Result<Branch<L, R>, Error> {
        let msg = self.1.recv().map_err(|_| Error::Disconnected)?;

        match msg {
            Message::Left => Ok(Branch::Left(Channel(self.0, self.1, PhantomData))),
            Message::Right => Ok(Branch::Right(Channel(self.0, self.1, PhantomData))),
            _ => return Err(Error::UnexpectedMessage),
        }
    }
}

#[test]
fn main() {
    type Protocol = Offer<Recv<i32, End>, Recv<bool, End>>;
    let (c1, c2) = new_session::<Protocol>();

    let n = 42;
    thread::spawn(move || {
        let c2 = c2.choose_left().unwrap();
        let c2 = c2.send(42).unwrap();
        c2.close();
    });

    match c1.offer().unwrap() {
        Branch::Left(c1) => {
            let (c1, _n) = c1.recv().unwrap();
            assert_eq!(n, _n);
            c1.close();
        }
        Branch::Right(_) => panic!("Expected left branch"),
    }
}
