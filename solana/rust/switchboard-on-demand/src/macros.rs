#[macro_export]
macro_rules! impl_account_deserialize {
    ($struct_name:ident) => {
        use anchor_client;
        use anchor_lang::prelude::{Error, ErrorCode};

        impl anchor_client::anchor_lang::AccountDeserialize for $struct_name {
            fn try_deserialize(buf: &mut &[u8]) -> Result<Self, Error> {
                if buf.len() < $struct_name::discriminator().len() {
                    return Err(ErrorCode::AccountDiscriminatorMismatch.into());
                }
                let given_disc = &buf[..8];
                if $struct_name::discriminator() != given_disc {
                    return Err(ErrorCode::AccountDiscriminatorMismatch.into());
                }
                Self::try_deserialize_unchecked(buf)
            }

            fn try_deserialize_unchecked(buf: &mut &[u8]) -> Result<Self, Error> {
                let data: &[u8] = &buf[8..];
                bytemuck::try_from_bytes(data)
                    .map(|r: &Self| *r)
                    .map_err(|_| ErrorCode::AccountDidNotDeserialize.into())
            }
        }
    };
}
