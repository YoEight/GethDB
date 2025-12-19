use crate::process::query::requirements::collect_requirements;
use crate::process::{
    Item, Managed, ProcessEnv,
    messages::{QueryRequests, QueryResponses},
};

#[tracing::instrument(skip_all, fields(proc_id = env.client.id(), proc = ?env.proc))]
pub async fn run(mut env: ProcessEnv<Managed>) -> eyre::Result<()> {
    while let Some(item) = env.recv().await {
        if let Item::Stream(stream) = item
            && let Ok(QueryRequests::Query { query }) = stream.payload.try_into()
        {
            let inferred = match geth_eventql::parse_rename_and_infer(&query) {
                Ok(q) => q,
                Err(e) => {
                    let _ = stream.sender.send(QueryResponses::Error(e.into()).into());
                    continue;
                }
            };

            let reqs = collect_requirements(inferred.query());
            let reader = env.client.new_reader_client().await?;
            // // let mut sources = HashMap::with_capacity(reqs.subjects.len());
            // let ctx = RequestContext::new();

            // for (binding, subjects) in reqs.subjects.iter() {
            //     for subject in subjects.iter() {
            //         // TODO - need to support true subject instead of relaying on stream name.
            //         let stream_name = subject.to_string();
            //         let _stream = reader
            //             .read(
            //                 ctx,
            //                 &stream_name,
            //                 Revision::Start,
            //                 Direction::Forward,
            //                 usize::MAX,
            //             )
            //             .await?;

            //         // TODO need to load them stream readers in a better way for all the sources.
            //     }
            // }
        }
    }

    Ok(())
}
