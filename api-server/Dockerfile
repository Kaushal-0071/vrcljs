FROM node:24-slim

RUN apt-get update

RUN apt-get install git -y

WORKDIR /home/app


COPY app.js app.js
COPY package*.json .
COPY kafka.pem kafka.pem
RUN npm install


RUN chmod +x app.js
EXPOSE 9000
CMD [ "node", "app.js" ]

