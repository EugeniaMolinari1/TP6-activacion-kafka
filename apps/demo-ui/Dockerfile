FROM node:22.19.0-alpine AS build
WORKDIR /app
COPY package*.json ./
RUN npm ci
COPY . .
ARG VITE_API_URL=http://localhost:3000
ENV VITE_API_URL=$VITE_API_URL
RUN npm run build
FROM nginx:1.28.0-alpine
COPY --from=build /app/dist /usr/share/nginx/html
EXPOSE 80
