// TODO(LNSD): Move to `thegraph` crate

use axum::http::{HeaderName, HeaderValue};
use headers::Error;
use thegraph_core::attestation::Attestation;

static GRAPH_ATTESTATION_HEADER_NAME: HeaderName = HeaderName::from_static("graph-attestation");

/// A typed header for the `graph-attestation` header.
///
/// The `graph-attestation` header value is a JSON encoded `Attestation` struct, or empty string
/// if no attestation is provided.
///
/// When deserializing the header value, if the value is empty, the header will be deserialized as
/// `None`. If the value is not empty, but cannot be deserialized as an `Attestation`, the header
/// is considered invalid.
#[derive(Debug, Clone)]
pub struct GraphAttestation(pub Option<Attestation>);

impl GraphAttestation {
    /// Create a new `GraphAttestation` header from the given attestation.
    #[cfg(test)]
    pub fn new(value: Attestation) -> Self {
        Self(Some(value))
    }

    /// Create a new empty `GraphAttestation` typed header.
    #[cfg(test)]
    pub fn empty() -> Self {
        Self(None)
    }
}

impl headers::Header for GraphAttestation {
    fn name() -> &'static HeaderName {
        &GRAPH_ATTESTATION_HEADER_NAME
    }

    fn decode<'i, I>(values: &mut I) -> Result<Self, Error>
    where
        Self: Sized,
        I: Iterator<Item = &'i HeaderValue>,
    {
        let value = values
            .next()
            .ok_or_else(Error::invalid)?
            .to_str()
            .map_err(|_| Error::invalid())?;

        if value.is_empty() {
            return Ok(Self(None));
        }

        let value = serde_json::from_str(value).map_err(|_| Error::invalid())?;

        Ok(Self(Some(value)))
    }

    fn encode<E: Extend<HeaderValue>>(&self, values: &mut E) {
        // Serialize the attestation as a JSON string, and convert it to a `HeaderValue`.
        // If the attestation is `None`, serialize an empty string.
        let value = self
            .0
            .as_ref()
            .and_then(|att| {
                serde_json::to_string(att)
                    .map(|s| HeaderValue::from_str(&s).unwrap())
                    .ok()
            })
            .unwrap_or_else(|| HeaderValue::from_static(""));

        values.extend(std::iter::once(value));
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use headers::{Header, HeaderValue};
    use thegraph_core::attestation::Attestation;

    use super::GraphAttestation;

    #[test]
    fn encode_attestation_into_header() {
        //* Given
        let attestation = Attestation {
            request_cid: Default::default(),
            response_cid: Default::default(),
            deployment: Default::default(),
            r: Default::default(),
            s: Default::default(),
            v: 0,
        };

        let mut headers = vec![];

        //* When
        let header = GraphAttestation::new(attestation.clone());

        header.encode(&mut headers);

        //* Then
        let value = headers
            .first()
            .expect("header to have been encoded")
            .to_str()
            .expect("header to be valid utf8");

        assert_matches!(serde_json::from_str::<Attestation>(value), Ok(att) => {
            assert_eq!(attestation.request_cid, att.request_cid);
            assert_eq!(attestation.response_cid, att.response_cid);
            assert_eq!(attestation.deployment, att.deployment);
            assert_eq!(attestation.r, att.r);
            assert_eq!(attestation.s, att.s);
            assert_eq!(attestation.v, att.v);
        });
    }

    #[test]
    fn encode_empty_attestation_header() {
        //* Given
        let mut headers = vec![];

        //* When
        let header = GraphAttestation::empty();

        header.encode(&mut headers);

        //* Then
        let value = headers
            .first()
            .expect("header to have been encoded")
            .to_str()
            .expect("header to be valid utf8");

        assert_eq!(value, "");
    }

    #[test]
    fn decode_attestation_from_valid_header() {
        //* Given
        let attestation = Attestation {
            request_cid: Default::default(),
            response_cid: Default::default(),
            deployment: Default::default(),
            r: Default::default(),
            s: Default::default(),
            v: 0,
        };

        let header = {
            let value = serde_json::to_string(&attestation).unwrap();
            HeaderValue::from_str(&value).unwrap()
        };
        let headers = [header];

        //* When
        let header = GraphAttestation::decode(&mut headers.iter());

        //* Then
        assert_matches!(header, Ok(GraphAttestation(Some(att))) => {
            assert_eq!(attestation.request_cid, att.request_cid);
            assert_eq!(attestation.response_cid, att.response_cid);
            assert_eq!(attestation.deployment, att.deployment);
            assert_eq!(attestation.r, att.r);
            assert_eq!(attestation.s, att.s);
            assert_eq!(attestation.v, att.v);
        });
    }

    #[test]
    fn decode_attestation_from_first_header() {
        //* Given
        let attestation = Attestation {
            request_cid: Default::default(),
            response_cid: Default::default(),
            deployment: Default::default(),
            r: Default::default(),
            s: Default::default(),
            v: 0,
        };

        let header = {
            let value = serde_json::to_string(&attestation).unwrap();
            HeaderValue::from_str(&value).unwrap()
        };
        let headers = [
            header,
            HeaderValue::from_static("invalid"),
            HeaderValue::from_static(""),
        ];

        //* When
        let header = GraphAttestation::decode(&mut headers.iter());

        //* Then
        assert_matches!(header, Ok(GraphAttestation(Some(_))));
    }

    #[test]
    fn decode_empty_attestation_from_valid_header() {
        //* Given
        let header = HeaderValue::from_static("");
        let headers = [header];

        //* When
        let header = GraphAttestation::decode(&mut headers.iter());

        //* Then
        assert_matches!(header, Ok(GraphAttestation(None)));
    }

    #[test]
    fn fail_decode_attestation_from_invalid_header() {
        //* Given
        let header = HeaderValue::from_static("invalid");
        let headers = [header];

        //* When
        let header = GraphAttestation::decode(&mut headers.iter());

        //* Then
        assert_matches!(header, Err(_));
    }

    #[test]
    fn fail_decode_attestation_if_no_headers() {
        //* Given
        let headers = [];

        //* When
        let header = GraphAttestation::decode(&mut headers.iter());

        //* Then
        assert_matches!(header, Err(_));
    }
}
