use crate::AmdSevSnpAttestation;
use anyhow::Error as AnyhowError;
#[cfg(feature = "verifier")]
use attestation_service::AttestationService;
use log::info;
#[cfg(feature = "verifier")]
use serde_json::json;
use serde_json::Value;
use sev_snp_utilities::guest::attestation::report::AttestationReport;
use sev_snp_utilities::{Policy, Verification};
use sha2::{Digest, Sha256};

impl AmdSevSnpAttestation {
    /// Verify the attestation report
    /// If message is empty skip the message verification
    #[deprecated(note = "for CoCo v0.15.0 use json_verify")]
    pub async fn verify(
        report: &AttestationReport,
        message: Option<&[u8]>,
    ) -> Result<(), AnyhowError> {
        info!("Starting old attestation verification");
        // Create strict policy for verification
        // let policy = Policy::strict();
        let policy = Policy::new(
            true,  // require_no_debug,
            true,  // require_no_ma,
            false, // require_no_smt,
            false, // require_id_key,
            false, // require_author_key,
        );

        // Verify report against policy
        report
            .verify(Some(policy))
            .await
            .map_err(|e| AnyhowError::msg(format!("Report verification failed: {}", e)))?;

        // If message is provided, verify it matches the report
        if let Some(msg) = message {
            let mut hasher = Sha256::new();
            hasher.update(msg);
            let msg_hash = hasher.finalize();
            //
            // Verify message hash matches report data
            if report.report_data[..] != msg_hash[..] {
                return Err(AnyhowError::msg(
                    "Message verification failed: hash mismatch",
                ));
            }
        }

        Ok(())
    }

    /// Verify the attestation report json
    /// If message is empty skip the message verification
    #[cfg(feature = "verifier")]
    pub async fn json_verify(
        report_json: Value,
        _message: Option<&[u8]>,
    ) -> Result<String, AnyhowError> {
        info!("Starting new (json) attestation verification");
        // Construct VerificationRequest
        let request = attestation_service::VerificationRequest {
            evidence: report_json,
            tee: attestation_service::Tee::Snp,
            runtime_data: None,
            runtime_data_hash_algorithm: attestation_service::HashAlgorithm::Sha256,
            init_data: None,
        };

        // Create a custom config with writable paths to avoid permission errors
        // Construct the config as JSON and deserialize it
        let config_json = json!({
            "work_dir": "/tmp/attestation-service",
            "rvps_config": {
                "type": "BuiltIn",
                "storage": {
                    "type": "LocalFs",
                    "file_path": "/tmp/attestation-service/reference_values"
                }
            }
        });

        let config: attestation_service::config::Config = serde_json::from_value(config_json)?;

        let service = AttestationService::new(config).await?;
        let token = service
            .evaluate(vec![request], vec!["default".to_string()])
            .await?;

        Ok(token)
    }

    /// Stub implementation when verifier feature is not enabled
    /// This allows compilation on platforms where attestation-service doesn't build (e.g., ARM Macs)
    #[cfg(not(feature = "verifier"))]
    pub async fn json_verify(
        _report_json: Value,
        _message: Option<&[u8]>,
    ) -> Result<String, AnyhowError> {
        Err(AnyhowError::msg(
            "Attestation verification not available: sail-sdk compiled without 'verifier' feature"
        ))
    }
}
