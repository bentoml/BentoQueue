<div align="center">
    <h1 align="center">Using BentoML as a Queue Consumer</h1>
</div>

This project demonstrates how to use BentoML as a Queue Consumer, enabling push-based inference instead of the traditional pull-based inference (i.e., HTTP Request). Specifically, this project utilizes [RabbitMQ](https://www.rabbitmq.com/) as a queue and [Amazon S3](https://aws.amazon.com/s3/) for artifact storage.

The Bento service listens for messages published to RabbitMQ. When a message is published (containing an image URL to S3), the Bento service downloads the image, processes it using a [CLIP model](https://github.com/openai/CLIP), and saves the output back to S3.

<div align="center">
    <img src="architecture.png" alt="Architecture Diagram" width="600">
</div>


## Prerequisites
- You have installed Python 3.8+ and `pip`. See the [Python downloads page](https://www.python.org/downloads/) to learn more.
- You have a basic understanding of key concepts in BentoML, such as Services. We recommend you read [Quickstart](https://docs.bentoml.com/en/1.2/get-started/quickstart.html) first.
- (Optional) We recommend you create a virtual environment for dependency isolation for this project. See the [Conda documentation](https://conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html) or the [Python documentation](https://docs.python.org/3/library/venv.html) for details.
- You need Docker installed to set up RabbitMQ. See the [Docker documentation](https://docs.docker.com/get-docker/) for installation instructions.
- You need an AWS account to use Amazon S3. See the [AWS documentation](https://aws.amazon.com/s3/) for more information.


## Install dependencies

### Installing RabbitMQ
```
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:management
```
### Installing Python Dependencies
```
git clone https://github.com/bentoml/neurolabs-stream.git
cd neurolabs-stream
pip install -r requirements.txt
```
### Setup S3 Bucket
- Log in to your AWS Management Console.
- Navigate to the S3 service.
- Click on "Create bucket".
- Enter a unique bucket name (e.g., `bento-queue`).
- Choose the AWS region (e.g., `us-west-1`).
- Leave the default settings for the rest of the options and click "Create bucket".

In your S3 bucket, you should have some images inside that can be downloaded for this project.

## Setup Environment Variables
```bash
export RABBITMQ_URL="amqp://guest:guest@localhost:5672/"
export S3_ACCESS_KEY=<your-s3-access-key>
export S3_SECRET_KEY=<your-s3-secret-key>

```

## Run the Service

### Run BentoML Service
We have defined a BentoML Service in `service.py`. Run `bentoml serve` in your project directory to start the Service.

```bash
$ bentoml serve .

2024-01-08T09:07:28+0000 [INFO] [cli] Prometheus metrics for HTTP BentoServer from "service:CLIPService" can be accessed at http://localhost:3000/metrics.
2024-01-08T09:07:28+0000 [INFO] [cli] Starting production HTTP BentoServer from "service:CLIPService" listening on http://localhost:3000 (Press CTRL+C to quit)
Model clip loaded device: cuda
```

### Produce Message to the Queue
```
python producer.py
```
You will be prompted to input a mesage to the queue, input the following and replace the images link in your S3 bucket respectively.
`[{"key": "images/my_image.png"}]`


## Deploy to BentoCloud

After the Service is ready, you can deploy the application to BentoCloud for better management and scalability. [Sign up](https://www.bentoml.com/) if you haven't got a BentoCloud account.

Make sure you have [logged in to BentoCloud](https://docs.bentoml.com/en/latest/bentocloud/how-tos/manage-access-token.html), then run the following command to deploy it.

```bash
bentoml deploy .
```

Once the application is up and running on BentoCloud, you can access it via the exposed URL.

**Note**: For custom deployment in your own infrastructure, use [BentoML to generate an OCI-compliant image](https://docs.bentoml.com/en/latest/guides/containerization.html).
