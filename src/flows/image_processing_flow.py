from prefect import flow, task

@task
def process_image(image_id: str):
    print(f"Processing image: {image_id}")

@flow
def image_processing_flow(image_id: str):
    process_image(image_id)

if __name__ == "__main__":
    image_processing_flow("sample_image_id")
