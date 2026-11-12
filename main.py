from pipelines.stripe_to_s3 import stripe_to_s3
from pipelines.postgres_to_s3 import postgres_to_s3    


if __name__ == "__main__":
    stripe_to_s3()
    postgres_to_s3()
