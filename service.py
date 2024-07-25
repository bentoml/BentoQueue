import asyncio
import json
import os
import pathlib
import sys
from typing import List

import bentoml
import numpy as np
from PIL.Image import open as open_image

MQ_URL = os.getenv("RABBITMQ_URL", "")
MQ_NAME = os.getenv("REBBITMQ_QUEUE_NAME", "test_queue")
SCHEDULE_CONCURRENCY = int(os.getenv("SCHEDULE_CONCURRENCY", 1000))


S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL", "https://s3.us-west-1.amazonaws.com")
S3_REGION_NAME = os.getenv("S3_REGION_NAME", "us-west-1")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "bento-queue")


MODEL_ID = "openai/clip-vit-base-patch32"


@bentoml.service(
    resources={"memory": "4Gi"},
    traffic={
        "concurrency": 5,
        "timeout": 300,
    },
)
class CLIP:
    def __init__(self) -> None:
        import boto3
        import torch
        from transformers import CLIPModel, CLIPProcessor

        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.model = CLIPModel.from_pretrained(MODEL_ID).to(self.device)  # type: ignore
        self.processor = CLIPProcessor.from_pretrained(MODEL_ID)
        self.logit_scale = (
            self.model.logit_scale.item() if self.model.logit_scale.item() else 4.60517
        )
        print("Model clip loaded", "device:", self.device)

        self.s3 = boto3.client(
            "s3",
            endpoint_url=S3_ENDPOINT_URL,
            region_name=S3_REGION_NAME,
            aws_access_key_id=S3_ACCESS_KEY,
            aws_secret_access_key=S3_SECRET_KEY,
        )

    def _local_path(self, ctx, key) -> str:
        temp_dir = pathlib.Path(ctx.temp_dir)
        local_path = temp_dir.joinpath(key)
        local_path.parent.mkdir(parents=True, exist_ok=True)
        return str(local_path)

    @bentoml.api(batchable=True)
    async def encode_image(self, items: List[dict], ctx: bentoml.Context) -> List[str]:
        images = []
        output_local_paths = []
        output_keys = []
        for item in items:
            key = item["key"]
            local_path = self._local_path(ctx, key)
            print(f"Downloading {key} to {local_path}")
            self.s3.download_file(S3_BUCKET_NAME, key, local_path)
            image = open_image(local_path)
            images.append(image)
            output_key = str(pathlib.Path(key).with_suffix(".npy"))
            output_keys.append(output_key)
            output_local_paths.append(self._local_path(ctx, output_key))

        inputs = self.processor(images=images, return_tensors="pt", padding=True).to(self.device)  # type: ignore
        image_embeddings = self.model.get_image_features(**inputs)  # type: ignore
        tensor = image_embeddings.cpu().detach().numpy()
        assert tensor.shape[0] == len(output_local_paths)

        for i, output_local_path in enumerate(output_local_paths):
            np.save(output_local_path, tensor[i])
            self.s3.upload_file(
                Bucket=S3_BUCKET_NAME,
                Key=output_keys[i],
                Filename=output_local_paths[i],
            )
        return output_keys


@bentoml.service()
class StreamApp:
    clip = bentoml.depends(CLIP)

    def __init__(self) -> None:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            loop.create_task(self.main(loop))
        self.semaphore = asyncio.Semaphore(SCHEDULE_CONCURRENCY)

    async def consume(self, body: dict, message):
        async with self.semaphore:
            try:
                result = await asyncio.gather(
                    *(self.clip.encode_image([b]) for b in body)  # type: ignore
                )
                assert result
                print("Result:", result)
                await message.ack()
            except Exception as e:
                await message.reject()
                print("Error:", e)

    async def main(self, loop):
        try:
            import aio_pika

            connection = await aio_pika.connect_robust(MQ_URL, loop=loop)
            print("Connected to RabbitMQ")

            async with connection:
                channel: aio_pika.abc.AbstractChannel = await connection.channel()
                print("Channel created")
                queue: aio_pika.abc.AbstractQueue = await channel.declare_queue(
                    MQ_NAME,
                    auto_delete=True,
                )
                print("Queue declared")

                async with queue.iterator() as queue_iter:
                    print("Listening for messages")
                    async for message in queue_iter:
                        print("Message received")
                        async with message.process():
                            print(message.body)

                            body_str = message.body.decode()
                            body = json.loads(body_str)
                            loop.create_task(self.consume(body, message))
        except Exception as e:
            print("Error:", e)
        finally:
            sys.exit(1)
