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

// Função para enviar pedidos do cliente para o nó
async function sendRequest() {
    const socket = new zmq.Push(); // Criado um socket
    await socket.connect(CLIENT_SEND); // Esse endereço é onde o nó do cluster está ouvindo para receber mensagens dos clientes
    let count = 0;// numero de pedidos enviados 
    
    while (true) {
        count++;
        const timestamp = Date.now(); // timestamp do no
        const msg = [`Pedido ${count} cliente ${NODE_ID}`, timestamp]; // mensagem a ser enviada
        await socket.send(JSON.stringify(msg)); // envia a mensagem
        console.log(`[Cliente ${NODE_ID}] Enviou: pedido ${count} | timestamp: ${timestamp}`); // pedido foi enviado
        await new Promise(resolve => setTimeout(resolve, 3000)); // aguarda 3 segundos antes de enviar a proxima msg
    }
}

// Função para receber respostas do nó
async function receiveResponse() {
    const socket = new zmq.Pull();// serve para receber mensagens dee um ou mais remetente
    await socket.bind(CLIENT_RECEIVE); // esse endereço é onde o cliente está ouvindo para receber msg do no
    
    for await (const [msg] of socket) { //espera msg do socket
        const response = JSON.parse(msg.toString()); // mgs convertida para json
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