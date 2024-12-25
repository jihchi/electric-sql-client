use reqwest::{header::HeaderMap, StatusCode, Url};

const ENDPOINT_SHAPE: &str = "/v1/shape";
const NEGATIVE_ONE: &str = "-1";

pub struct ShapeStream {
    cursor: Option<String>,
    handle: Option<String>,
    host: String,
    is_up_to_date: bool,
    offset: String,
    table: String,
}

impl ShapeStream {
    pub fn new(host: &str, table: &str, offset: Option<&str>) -> Self {
        let offset = offset.unwrap_or(NEGATIVE_ONE).to_string();

        Self {
            cursor: None,
            handle: None,
            host: host.to_string(),
            is_up_to_date: false,
            offset,
            table: table.to_string(),
        }
    }
}

impl ShapeStream {
    fn set_offset(&mut self, headers: &HeaderMap) {
        self.offset = headers
            .get("electric-offset")
            .expect("electric-offset is missing in the header")
            .to_str()
            .expect("header electric-offset contains non-ASCII chars")
            .to_string();
    }

    fn set_handle(&mut self, headers: &HeaderMap) {
        self.handle = headers.get("electric-handle").map(|header| {
            header
                .to_str()
                .expect("header electric-handle contains non-ASCII chars")
                .to_string()
        });
    }

    fn set_cursor(&mut self, headers: &HeaderMap) {
        self.cursor = headers.get("electric-cursor").map(|header| {
            header
                .to_str()
                .expect("header electric-cursor contains non-ASCII chars")
                .to_string()
        });
    }

    fn set_is_up_to_date(&mut self, headers: &HeaderMap) {
        self.is_up_to_date = headers.get("electric-up-to-date").is_some();
    }

    fn get_url(&self) -> Result<Url, Box<dyn std::error::Error>> {
        let base = Url::parse(&self.host)?;
        let mut url = base.join(ENDPOINT_SHAPE)?;

        url.query_pairs_mut()
            .append_pair("table", &self.table)
            .append_pair("offset", &self.offset);

        if let Some(handle) = &self.handle {
            url.query_pairs_mut().append_pair("handle", handle);
        }

        if self.is_up_to_date {
            if let Some(cursor) = &self.cursor {
                url.query_pairs_mut().append_pair("cursor", cursor);
            }
            url.query_pairs_mut().append_pair("live", "true");
        }

        Ok(url)
    }

    pub async fn fetch(&mut self) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let mut messages = Vec::new();

        loop {
            let url = self.get_url()?;
            let resp = reqwest::get(url).await?;
            let status = resp.status();

            if !status.is_success() {
                eprintln!("request is unsuccessful: {}", status);
                break;
            }

            let headers = resp.headers();

            self.set_offset(headers);
            self.set_handle(headers);
            self.set_cursor(headers);
            self.set_is_up_to_date(headers);

            if status == StatusCode::NO_CONTENT {
                continue;
            }

            let body = resp.text().await?;
            messages.push(body);

            if self.is_up_to_date {
                break;
            }
        }

        Ok(messages)
    }
}
