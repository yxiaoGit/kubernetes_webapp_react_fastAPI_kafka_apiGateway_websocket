# Step 1: Compile React App
FROM node:18-alpine AS builder
WORKDIR /app

# Install standard React build structures
RUN npm install -g create-react-app && npx create-react-app . || true

# --- THE FIX: WIPE BOILERPLATE AND FORCE COPY YOUR CUSTOM FILE ---
RUN rm -f src/App.js src/App.tsx src/App.css
COPY App.js ./src/App.js
# -----------------------------------------------------------------

# Build the highly optimized static production asset bundle
RUN npm run build || mkdir dist && cp -r build/* dist/ || true

# Step 2: Serve via Lightweight Nginx Container
FROM nginx:alpine
COPY --from=builder /app/build /usr/share/nginx/html
EXPOSE 80

