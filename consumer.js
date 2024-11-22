const amqp = require('amqplib');
const WebSocket = require('ws');
const { exec } = require('child_process');

// Configuración de RabbitMQ
const amqpUrl = 'amqp://donetrm:brandi12@3.90.75.187'; // IP del servidor RabbitMQ con credenciales
const queue = 'test.rabbitmq.direct'; // Nombre de la cola

let wss;

// Configuración del servidor WebSocket
const setupWebSocketServer = () => {
  wss = new WebSocket.Server({ port: 8080 });

  wss.on('connection', (ws) => {
    console.log('New client connected');

    ws.on('close', () => {
      console.log('Client disconnected');
    });

    ws.on('error', (err) => {
      console.error('WebSocket error:', err);
    });
  });

  console.log('WebSocket server started on port 8080');
};

// Función para enviar datos a los clientes WebSocket
const broadcastData = (data) => {
  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(JSON.stringify(data));
    }
  });
};

// Función para iniciar el consumidor RabbitMQ
const startConsumer = async () => {
  try {
    const connection = await amqp.connect(amqpUrl);
    const channel = await connection.createChannel();

    await channel.assertQueue(queue, { durable: true });

    console.log('Waiting for messages in %s. To exit press CTRL+C', queue);

    channel.consume(queue, (msg) => {
      if (msg !== null) {
        try {
          const data = JSON.parse(msg.content.toString());
          console.log('Received:', data);

          // Validar datos
          if (data.temperature != null && data.humidity != null && data.lightLevel != null) {
            broadcastData(data);
          } else {
            console.error('Invalid data format:', data);
          }

          // Confirmar recepción del mensaje
          channel.ack(msg);
        } catch (err) {
          console.error('Error processing message:', err);
        }
      }
    });

    connection.on('close', () => {
      console.error('Connection closed, reconnecting...');
      setTimeout(startConsumer, 1000);
    });

    connection.on('error', () => {
      console.error('Connection error, reconnecting...');
      setTimeout(startConsumer, 1000);
    });
  } catch (error) {
    console.error('Error in RabbitMQ consumer:', error);
    setTimeout(startConsumer, 1000);
  }
};

// Iniciar servicios
setupWebSocketServer();
startConsumer();