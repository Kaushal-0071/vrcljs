FROM node:24-slim

RUN apt-get update

RUN apt-get install git -y

WORKDIR /home/app

COPY main.sh main.sh
COPY script.js script.js
COPY package*.json .
COPY kafka.pem kafka.pem
RUN npm install

RUN chmod +x main.sh
RUN chmod +x script.js
EXPOSE 3001
ENTRYPOINT [ "/home/app/main.sh" ]

