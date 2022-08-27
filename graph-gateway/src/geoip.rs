use maxminddb::{geoip2, MaxMindDBError, Reader};
use std::{net::IpAddr, path::Path};

pub struct GeoIP {
    reader: Reader<Vec<u8>>,
    blocked_countries: Vec<String>,
}

impl GeoIP {
    pub fn new(
        db_file: impl AsRef<Path>,
        blocked_countries: Vec<String>,
    ) -> Result<Self, MaxMindDBError> {
        let reader = Reader::open_readfile(db_file)?;
        Ok(Self {
            reader,
            blocked_countries,
        })
    }

    pub fn is_ip_blocked(&self, address: IpAddr) -> bool {
        let country = match self.reader.lookup::<geoip2::Country>(address) {
            Ok(country) => country,
            Err(geoip_lookup_err) => {
                tracing::error!(geoip_lookup_err = %format!("{geoip_lookup_err} ({address})"));
                return false;
            }
        };
        let iso_code = match country.country.and_then(|c| c.iso_code) {
            Some(iso_code) => iso_code,
            None => return false,
        };
        self.blocked_countries.iter().any(|code| code == iso_code)
    }
}
