# replica/Dockerfile
FROM node:18

WORKDIR /app

COPY package*.json ./
RUN npm install

COPY . .

EXPOSE 5001 5002 5003

CMD ["node", "server.js"]