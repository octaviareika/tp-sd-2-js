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

// Função para receber a msg do cliente
async function clientReceiverThread() {
    const sock = new zmq.Pull(); // serve para criar o socket
    await sock.bind(CLIENT_RECEIVE); // conecta com o cliente para receber o pedido do cliente

    for await (const [msg] of sock) {
        const message = JSON.parse(msg.toString()); // converte a msg em string
        console.log(`[Nó ${NODE_ID}] Recebeu pedido do cliente: ${message[0]} | timestamp: ${message[1]}`);
        receivedBuffer.push(message);
    }
}

// Mandar msg para o cliente
async function clientSenderThread() {
    const sock = new zmq.Push();
    await sock.connect(CLIENT_SEND); // manda para o endereço do cliente, esse endereço é onde o cliente vai ouvir as msgs

    while (true) {
        await sleep(5000); // espera de 5 segundos 
        if (processedBuffer.length > 0) {
            // buffer pega a primeira msg em envia para o cliente e dps fica vazio
            const msg = processedBuffer.shift(); // buffer ou acumulador, se nao tiver vazio a funcao remove a primeira msg do buffer
            await sock.send(JSON.stringify(msg)); // converte em json e manda pro cliente 
            console.log(`[Nó ${NODE_ID}] Enviou resposta para o cliente: ${msg[0]}`);
        }
    }
}

async function tokenReceiverThread() {
    await sleep(1000);
    const sock = new zmq.Pull(); // token recebe msg dos remetentes
    await sock.bind(MY_TOKEN_ADDR); // esse endereço é onde o nó está ouvindo para receber o token

    for await (const [msg] of sock) { // aguarda msg do socket 
        let token = JSON.parse(msg.toString()); // processa o token
        console.log(`[Nó ${NODE_ID}] Recebeu token`);
        await sleep(Math.random() * 800 + 200);

        // quando o cliente manda msg, esta é armazenada em um token que vai ser criado dependendo do numero de nós
        if (token[NODE_ID][1] !== -1) { // se o token contem um pedido atual para o nó
            let isSmallest = token.every(entry => entry[1] === -1 || entry[1] >= token[NODE_ID][1]);
            if (isSmallest) { // verifica qual nó tem o menor timestamp
                console.log(`[Nó ${NODE_ID}] Entrando na zona crítica`);
                
                while (receivedBuffer.length > 0) {
                    const request = receivedBuffer.shift(); // processa todos os pedidos do cliente no buffer recebido e armazena no receveid
                    processedBuffer.push(await requestProcessingThread(request));
                }
                
                token[NODE_ID] = ["", -1];
                console.log(`[Nó ${NODE_ID}] Saindo da zona crítica`);
            }
        }
        
        if (receivedBuffer.length > 0 && token[NODE_ID][1] === -1) {
            token[NODE_ID] = receivedBuffer.shift(); // se o token atual nao tem nenhum pedido armazenado, o nó adiciona o novo pedido ao token
        }
        
        // passar o token para o próximo nó
        const nextSock = new zmq.Push(); // cria um socket para enviar o token
        await nextSock.connect(NEXT_NODE); // conecta ao endereço para o proximo no
        await nextSock.send(JSON.stringify(token)); // O token é enviado para o próximo nó,
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
        tokenReceiverThread(), // Função que recebe e processa tokens de outros nós.
        tokenCreatorThread() ///  Função que cria o token inicial
    ]);
})();