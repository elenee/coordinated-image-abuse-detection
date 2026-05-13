FROM node:22-alpine

WORKDIR /app

COPY package*.json ./

COPY prisma ./prisma/

RUN npm install

COPY . .

RUN npm run build

RUN ls -al /app/dist

RUN mkdir -p /uploads

CMD ["node", "dist/src/main.js"]