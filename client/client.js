const zmq = require("zeromq");
const os = require("os");

// Obtém o nome do host e extrai o ID do nó
function getPodId() {
    const hostname = os.hostname();
    const match = hostname.match(/client-node-(\d+)/);
    return match ? parseInt(match[1], 10) : -1;
}

const NODE_ID = getPodId();
const CLIENT_PORT = 7000;
const CLIENT_RECEIVE = `tcp://client-node-${NODE_ID}.client-service.default.svc.cluster.local:${CLIENT_PORT}`;
const CLIENT_SEND = `tcp://cluster-node-${NODE_ID}.cluster-node-service.default.svc.cluster.local:${CLIENT_PORT}`;

// Função para enviar pedidos para o nó
async function sendRequest() {
    const socket = new zmq.Push();
    await socket.connect(CLIENT_SEND); // connect é para enviar mensagens, conecta ao endereço
    let count = 0;
    
    while (true) {
        count++;
        const timestamp = Date.now();
        const msg = [`Pedido ${count} cliente ${NODE_ID}`, timestamp]; // mensagem a ser enviada
        await socket.send(JSON.stringify(msg)); // envia a mensagem
        console.log(`[Cliente ${NODE_ID}] Enviou: pedido ${count} | timestamp: ${timestamp}`);
        await new Promise(resolve => setTimeout(resolve, 3000));
    }
}

// Função para receber respostas do nó
async function receiveResponse() {
    const socket = new zmq.Pull();// serve para receber mensagens
    await socket.bind(CLIENT_RECEIVE); // bind é para receber mensagens
    
    for await (const [msg] of socket) {
        const response = JSON.parse(msg.toString());
        console.log(`[Cliente ${NODE_ID}] Resposta recebida: ${response[0]} concluído | timestamp: ${response[1]}`);
    }
}

// Inicia as funções em paralelo
(async () => {
    setTimeout(() => {
        sendRequest();
        receiveResponse();
    }, 5000);
})();