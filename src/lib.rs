use async_stream::try_stream;
use futures::Stream;
use reqwest::{header::HeaderMap, StatusCode, Url};

const ENDPOINT_SHAPE: &str = "/v1/shape";
const NEGATIVE_ONE: &str = "-1";

enum Mode {
    InitialSync {
        offset: Option<String>,
        handle: Option<String>,
    },
    Live {
        offset: String,
        handle: String,
        cursor: Option<String>,
    },
}

pub struct ShapeStream {
    mode: Mode,
    host: String,
    table: String,
}

impl ShapeStream {
    pub fn new(host: &str, table: &str, offset: Option<&str>) -> Self {
        let offset = offset.map(|offset| offset.to_string());

        Self {
            mode: Mode::InitialSync {
                offset,
                handle: None,
            },
            host: host.to_string(),
            table: table.to_string(),
        }
    }

    fn get_offset(&self) -> String {
        match &self.mode {
            Mode::InitialSync { offset, handle: _ } => match offset {
                Some(offset) => offset.clone(),
                None => NEGATIVE_ONE.to_string(),
            },
            Mode::Live {
                offset,
                handle: _,
                cursor: _,
            } => offset.clone(),
        }
    }

    fn get_handle(&self) -> Option<String> {
        match &self.mode {
            Mode::InitialSync { offset: _, handle } => handle.clone(),
            Mode::Live {
                offset: _,
                handle,
                cursor: _,
            } => Some(handle.clone()),
        }
    }

    fn get_cursor(&self) -> Option<String> {
        match &self.mode {
            Mode::InitialSync {
                offset: _,
                handle: _,
            } => None,
            Mode::Live {
                offset: _,
                handle: _,
                cursor,
            } => cursor.clone(),
        }
    }

    fn get_url(&self) -> Result<Url, Box<dyn std::error::Error>> {
        let base = Url::parse(&self.host)?;
        let mut url = base.join(ENDPOINT_SHAPE)?;

        url.query_pairs_mut()
            .append_pair("table", &self.table)
            .append_pair("offset", &self.get_offset());

        if let Some(handle) = &self.get_handle() {
            url.query_pairs_mut().append_pair("handle", handle);
        }

        if let Some(cursor) = &self.get_cursor() {
            url.query_pairs_mut().append_pair("cursor", cursor);
        }

        match &self.mode {
            Mode::InitialSync {
                offset: _,
                handle: _,
            } => (),
            Mode::Live {
                offset: _,
                handle: _,
                cursor: _,
            } => {
                url.query_pairs_mut().append_pair("live", "true");
            }
        }

        Ok(url)
    }

    fn set_to_sync_mode(&mut self, headers: &HeaderMap) {
        let offset = ShapeStream::extract_offset(headers);
        let handle = ShapeStream::extract_handle(headers);
        self.mode = Mode::InitialSync { offset, handle };
    }

    fn set_to_live_mode(&mut self, headers: &HeaderMap) {
        let offset =
            ShapeStream::extract_offset(headers).expect("live mode requires offset in headers");

        let handle =
            ShapeStream::extract_handle(headers).expect("live mode requires handle in headers");

        let cursor = ShapeStream::extract_cursor(headers);

        self.mode = Mode::Live {
            offset,
            handle,
            cursor,
        };
    }

    async fn initial_sync(&mut self) -> Result<String, Box<dyn std::error::Error>> {
        let mut result = String::new();

        loop {
            let url = self.get_url()?;
            let resp = reqwest::get(url).await?;
            let status = resp.status();

            if !status.is_success() {
                panic!("request is unsuccessful: {}", status);
            }

            let headers = resp.headers();
            let is_up_to_date = ShapeStream::is_up_to_date(headers);

            if is_up_to_date {
                self.set_to_live_mode(headers);
            } else {
                self.set_to_sync_mode(headers);
            }

            let body = resp.text().await?;
            result.push_str(&body);
            result.push('\n');

            if is_up_to_date {
                return Ok(result);
            }
        }
    }

    async fn live(&mut self) -> Result<String, Box<dyn std::error::Error>> {
        let mut result = String::new();

        loop {
            let url = self.get_url()?;
            let resp = reqwest::get(url).await?;
            let status = resp.status();

            if !status.is_success() {
                panic!("request is unsuccessful: {}", status);
            }

            let headers = resp.headers();
            let is_up_to_date = ShapeStream::is_up_to_date(headers);

            self.set_to_live_mode(headers);

            if status == StatusCode::NO_CONTENT {
                continue;
            }

            let body = resp.text().await?;
            result.push_str(&body);
            result.push('\n');

            if is_up_to_date {
                return Ok(result);
            }
        }
    }

    pub fn run(
        &mut self,
    ) -> impl Stream<Item = Result<String, Box<dyn std::error::Error>>> + use<'_> {
        try_stream! {
                let logs = self.initial_sync().await?;
                yield logs;

                loop {
                    let logs = self.live().await?;
                    yield logs;
                }
        }
    }
}

impl ShapeStream {
    fn is_up_to_date(headers: &HeaderMap) -> bool {
        headers.get("electric-up-to-date").is_some()
    }

    fn extract_handle(headers: &HeaderMap) -> Option<String> {
        headers.get("electric-handle").map(|header| {
            header
                .to_str()
                .expect("header electric-handle contains non-ASCII chars")
                .to_string()
        })
    }

    fn extract_offset(headers: &HeaderMap) -> Option<String> {
        headers.get("electric-offset").map(|header| {
            header
                .to_str()
                .expect("header electric-offset contains non-ASCII chars")
                .to_string()
        })
    }

    fn extract_cursor(headers: &HeaderMap) -> Option<String> {
        headers.get("electric-cursor").map(|header| {
            header
                .to_str()
                .expect("header electric-cursor contains non-ASCII chars")
                .to_string()
        })
    }
}
