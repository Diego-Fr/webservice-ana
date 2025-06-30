FROM node:20-alpine as base

WORKDIR /.
COPY package*.json /

FROM base as production
ENV NODE_ENV=production
RUN npm ci
COPY . .
CMD ["node", "index.js"]