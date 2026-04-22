const axios = require('axios');
const HttpsProxyAgent = require('https-proxy-agent');

if(process.env.enviroment === 'production' && process.env.HTTP_PROXY){
    const proxyAgent = new HttpsProxyAgent(process.env.HTTP_PROXY);
    axios.defaults.httpAgent = proxyAgent;
    axios.defaults.httpsAgent = proxyAgent;
    axios.defaults.proxy = false; // importantíssimo
}

module.exports = axios;