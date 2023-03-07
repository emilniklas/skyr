#[derive(Debug, Default)]
pub struct Plan<'a> {
    _tmp: Option<&'a str>,
}

impl<'a> Plan<'a> {
    pub fn new() -> Self {
        Plan { _tmp: None }
    }

    pub async fn execute(self) -> Option<Plan<'a>> {
        println!("Executing plan!");
        None
    }
}
