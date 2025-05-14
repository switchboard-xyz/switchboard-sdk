use std::fmt;

#[cfg(feature = "anchor")]
use anchor_lang::prelude::*;
use serde::ser::StdError;

#[cfg_attr(feature = "anchor", error_code)]
#[cfg_attr(not(feature = "anchor"), derive(Clone, Debug))]
#[cfg_attr(not(feature = "anchor"), repr(u32))]
pub enum OnDemandError {
    Generic,
    AccountBorrowError,
    AccountNotFound,
    AnchorParse,
    AnchorParseError,
    CheckSizeError,
    DecimalConversionError,
    DecryptError,
    EventListenerRoutineFailure,
    EvmError,
    FunctionResultIxIncorrectTargetChain,
    HeartbeatRoutineFailure,
    IntegerOverflowError,
    InvalidChain,
    InvalidData,
    InvalidDiscriminator,
    InvalidInstructionError,
    InvalidKeypairFile,
    InvalidNativeMint,
    InvalidQuote,
    InvalidQuoteError,
    InvalidSignature,
    IpfsNetworkError,
    IpfsParseError,
    KeyParseError,
    MrEnclaveMismatch,
    NetworkError,
    ParseError,
    PdaDerivationError,
    QuoteParseError,
    QvnTxSendFailure,
    SgxError,
    SgxWriteError,
    SolanaBlockhashError,
    SolanaMissingSigner,
    SolanaPayerSignerMissing,
    SolanaPayerMismatch,
    SolanaInstructionOverflow,
    SolanaInstructionsEmpty,
    TxCompileErr,
    TxDeserializationError,
    TxFailure,
    Unexpected,
    SolanaSignError,
    IoError,
    KeyDerivationFailed,
    InvalidSecretKey,
    EnvVariableMissing,
    AccountDeserializeError,
    NotEnoughSamples,
    IllegalFeedValue,
    SwitchboardRandomnessTooOld,
    AddressLookupTableFetchError,
    AddressLookupTableDeserializeError,
    InvalidSize,
    StaleResult,
}

impl StdError for OnDemandError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        None
    }
}
#[cfg(not(feature = "anchor"))]
impl fmt::Display for OnDemandError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:#?}", self)
    }
}
