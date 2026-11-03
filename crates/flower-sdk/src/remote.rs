//! Contract-only remote descriptors. No guest callback or provider implementation.
use crate::{
    BusinessError, FlowerType, Op,
    error::{Result, codes, fail},
};
use core::marker::PhantomData;
type DescriptorTypes<A, T, E> = PhantomData<fn() -> (A, T, E)>;

pub struct QueryOp<A, T, E = crate::Failure> {
    pub name: &'static str,
    types: DescriptorTypes<A, T, E>,
}
pub struct MutationOp<A, T, E = crate::Failure> {
    pub name: &'static str,
    types: DescriptorTypes<A, T, E>,
}
macro_rules! descriptor {
    ($name:ident) => {
        impl<A, T, E> Copy for $name<A, T, E> {}
        impl<A, T, E> Clone for $name<A, T, E> {
            fn clone(&self) -> Self {
                *self
            }
        }
        impl<A: FlowerType, T: FlowerType, E: BusinessError> $name<A, T, E> {
            pub const fn new(name: &'static str) -> Self {
                Self {
                    name,
                    types: PhantomData,
                }
            }
        }
    };
}
descriptor!(QueryOp);
descriptor!(MutationOp);

/// Backend for generated native interface clients. The SDK stays `core` + `alloc`;
/// the backend chooses its futures, result metadata, transport and error types.
pub trait RemoteModule {
    type Query<'a, A: FlowerType + Sync, T: FlowerType, E: BusinessError>: core::future::IntoFuture
    where
        Self: 'a;
    type Prepare<'a, A: FlowerType + Sync, T: FlowerType, E: BusinessError>: core::future::Future
    where
        Self: 'a;

    fn query<'a, A: FlowerType + Sync, T: FlowerType, E: BusinessError>(
        &'a self,
        op: QueryOp<A, T, E>,
        args: &'a A,
    ) -> Self::Query<'a, A, T, E>;
    fn prepare<'a, A: FlowerType + Sync, T: FlowerType, E: BusinessError>(
        &'a self,
        op: MutationOp<A, T, E>,
        args: &'a A,
    ) -> Self::Prepare<'a, A, T, E>;
}

/// Optional command-builder backend for generated native clients.
pub trait RemoteCommands: RemoteModule {
    type Command<'a, A: FlowerType + Sync, T: FlowerType, E: BusinessError>
    where
        Self: 'a;
    fn command<'a, A: FlowerType + Sync, T: FlowerType, E: BusinessError>(
        &'a self,
        op: MutationOp<A, T, E>,
        args: &'a A,
    ) -> Self::Command<'a, A, T, E>;
}

impl<A: FlowerType, T: FlowerType, E: BusinessError> Op<A, T, E> {
    pub fn as_query(&self) -> Result<QueryOp<A, T, E>> {
        if !self.is_query() {
            return Err(fail(
                codes::INVALID_INVOCATION,
                "mutation cannot be queried or watched",
            ));
        }
        Ok(QueryOp::new(self.name))
    }
    pub fn as_mutation(&self) -> Result<MutationOp<A, T, E>> {
        if self.is_query() {
            return Err(fail(
                codes::INVALID_INVOCATION,
                "query cannot be submitted as a command",
            ));
        }
        Ok(MutationOp::new(self.name))
    }
}
