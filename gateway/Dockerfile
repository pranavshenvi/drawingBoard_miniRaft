FROM node:18

WORKDIR /app

COPY gateway/package*.json ./

RUN npm install

COPY gateway/ .

COPY fontend ./fontend

EXPOSE 8080

CMD ["node", "server.js"]