1. Build Docker image
> docker build -t antipode-lambda .

2. Run docker container
> docker run --name antipode-lambda --rm -ti -v "$(pwd):/app" antipode-lambda bash

3. Demo example run:
```
./antipode_lambda build \
    --post-storage mysql \
    --notification-storage sns \
    --writer eu \
    --reader us \
    -ant

./antipode_lambda run -r 5000

./antipode_lambda gather

./antipode_lambda clean --strong
```