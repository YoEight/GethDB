use crate::process::{Item, ProcessEnv};
use bytes::{Buf, BufMut};

pub async fn run(mut env: ProcessEnv) -> eyre::Result<()> {
    while let Some(item) = env.queue.recv().await {
        if let Item::Stream(mut stream) = item {
            let low = stream.payload.get_u64_le();
            let high = stream.payload.get_u64_le();

            for num in low..high {
                env.buffer.put_u64_le(num);
                if stream.sender.send(env.buffer.split().freeze()).is_err() {
                    break;
                }
            }
        }
    }

    Ok(())
}
