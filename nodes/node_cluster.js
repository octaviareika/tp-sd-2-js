const zmq = require("zeromq");
const os = require("os");
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// Configuração da rede
const TOKEN_PORT = 6000;
const CLIENT_PORT = 7000;
const TOTAL_NODES = 5;

// Obtém o ID do nó a partir do hostname
function getPodId() {
    const hostname = os.hostname();
    const match = hostname.match(/cluster-node-(\d+)/);
    return match ? parseInt(match[1], 10) : -1;
}

const NODE_ID = getPodId();
const MY_TOKEN_ADDR = `tcp://cluster-node-${NODE_ID}.cluster-node-service.default.svc.cluster.local:${TOKEN_PORT}`;
const NEXT_NODE = `tcp://cluster-node-${(NODE_ID + 1) % TOTAL_NODES}.cluster-node-service.default.svc.cluster.local:${TOKEN_PORT}`;
const CLIENT_RECEIVE = `tcp://cluster-node-${NODE_ID}.cluster-node-service.default.svc.cluster.local:${CLIENT_PORT}`;
const CLIENT_SEND = `tcp://client-node-${NODE_ID}.client-service.default.svc.cluster.local:${CLIENT_PORT}`;

const receivedBuffer = [];
const processedBuffer = [];

async function clientReceiverThread() {
    const sock = new zmq.Pull(); // serve para criar o socket
    await sock.bind(CLIENT_RECEIVE); // conecta com o cliente para receber o pedido do cliente

    for await (const [msg] of sock) {
        const message = JSON.parse(msg.toString()); // converte a msg em string
        console.log(`[Nó ${NODE_ID}] Recebeu pedido do cliente: ${message[0]} | timestamp: ${message[1]}`);
        receivedBuffer.push(message);
    }
}

async function clientSenderThread() {
    const sock = new zmq.Push();
    await sock.connect(CLIENT_SEND);

    while (true) {
        await sleep(5000);
        if (processedBuffer.length > 0) {
            const msg = processedBuffer.shift();
            await sock.send(JSON.stringify(msg));
            console.log(`[Nó ${NODE_ID}] Enviou resposta para o cliente: ${msg[0]}`);
        }
    }
}

async function tokenReceiverThread() {
    await sleep(1000);
    const sock = new zmq.Pull();
    await sock.bind(MY_TOKEN_ADDR);

    for await (const [msg] of sock) {
        let token = JSON.parse(msg.toString());
        console.log(`[Nó ${NODE_ID}] Recebeu token`);
        await sleep(Math.random() * 800 + 200);

        if (token[NODE_ID][1] !== -1) {
            let isSmallest = token.every(entry => entry[1] === -1 || entry[1] >= token[NODE_ID][1]);
            if (isSmallest) {
                console.log(`[Nó ${NODE_ID}] Entrando na zona crítica`);
                
                while (receivedBuffer.length > 0) {
                    const request = receivedBuffer.shift();
                    processedBuffer.push(await requestProcessingThread(request));
                }
                
                token[NODE_ID] = ["", -1];
                console.log(`[Nó ${NODE_ID}] Saindo da zona crítica`);
            }
        }
        
        if (receivedBuffer.length > 0 && token[NODE_ID][1] === -1) {
            token[NODE_ID] = receivedBuffer.shift();
        }
        
        const nextSock = new zmq.Push();
        await nextSock.connect(NEXT_NODE);
        await nextSock.send(JSON.stringify(token));
        console.log(`[Nó ${NODE_ID}] Enviou token para o próximo nó`);
    }
}

async function tokenCreatorThread() {
    if (NODE_ID === 4) {
        await sleep(2000); // espera de 2 segundos 
        const sock = new zmq.Push();
        await sock.connect(NEXT_NODE);

        console.log(`[Nó ${NODE_ID}] Criou o token`);
        await sleep(1000);
        const token = Array(TOTAL_NODES).fill(["", -1]);
        await sock.send(JSON.stringify(token));
        console.log(`[Nó ${NODE_ID}] Iniciou o token`);
    }
}

async function requestProcessingThread(message) {
    await sleep(Math.random() * 800 + 200);
    console.log(`[Nó ${NODE_ID}] Requisição processada ${message[1]}`);
    return message;
}

(async () => {
    await Promise.all([
        clientReceiverThread(),
        clientSenderThread(),
        tokenReceiverThread(),
        tokenCreatorThread()
    ]);
})();