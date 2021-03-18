FROM node:14-buster-slim

ENV NODE_ENV="production"
ENV NO_UPDATE_NOTIFIER="true"

WORKDIR /app
COPY ["package.json", "package-lock.json*", "./"]
RUN npm install --production

COPY src src

EXPOSE 3001

ENTRYPOINT ["npm", "run", "start"]