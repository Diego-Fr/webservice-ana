require('dotenv').config()
const { default: axios } = require("axios")

const rateLimit = require('axios-rate-limit');
const moment = require('moment');
const axios_queue = rateLimit(axios.create(), { maxRPS: 15 });
var CronJob = require('cron').CronJob;

const pidusage = require('pidusage');

let CURRENT_ANA_TOKEN = ''
let SIBH_STATIONS = {}
let PROMISES = []
let MEASUREMENTS = []
let STATION_LAST_MEASUREMENT = {}

const pgp = require('pg-promise')({
    /* initialization options */
    capSQL: true // capitalize all generated SQL
});

const db = pgp({
    connectionString: 'postgres://'+process.env.DB_USER+':'+process.env.DB_PASSWORD+'@'+process.env.DB_HOST+':'+process.env.DB_PORT+'/'+process.env.DB_DATABASE
});

const cs = new pgp.helpers.ColumnSet(
    ['station_prefix_id','date_hour','value','read_value', 'measurement_classification_type_id','transmission_type_id','information_origin'],
    {table: 'measurements'}
);

var job_default = new CronJob(
    process.env.CRON_PATTERN,
	function() {
        start()
	},
	null,
	true
);

console.log('Started');



// setInterval(() => {
//   pidusage(process.pid, (err, stats) => {
//     console.log(stats); // { cpu, memory, pid, ctime, elapsed, timestamp }
//   });
// }, 5000);

const start = async () =>{

    PROMISES = []
    STATION_LAST_MEASUREMENT = {}

    await getANASibhStations()
    await tryAuthenticate()
    if(CURRENT_ANA_TOKEN && Object.keys(SIBH_STATIONS).length > 0){
        console.log('Token e postos carregados com sucesso, buscando medições');
        
        let LIST = Object.values(SIBH_STATIONS)

        console.log('Buscando medições', LIST.length, ' postos');
        
        for(let i = 0; i< LIST.length; i++){
            let station = LIST[i]
            PROMISES.push(getMeasurements(station, moment().format('YYYY-MM-DD'), 'HORA_6'))
            
        }

        
    } else {
       console.log('Sem token ou postos para consultar dados');
        
    }

    //esperand todas as requisições pro webservice finalizar
    await Promise.all(PROMISES)

    await saveMeasurements()

    await updateDateLastMeasurement()
    
    
}

const saveMeasurements = async () =>{
    console.log('Salvando medições', MEASUREMENTS.length);

    while(MEASUREMENTS.length > 0){
        let measurements = MEASUREMENTS.splice(0,1000) //de mil em mil

        await saveChunk(measurements)
    }

    
    
}

const saveChunk = async measurements =>{
    if(measurements.length > 0){
        
        const query = pgp.helpers.insert(measurements, cs) + " ON CONFLICT (date_hour, station_prefix_id, transmission_type_id) DO UPDATE SET value = EXCLUDED.value, read_value = EXCLUDED.read_value, information_origin = 'WS-ANA-NODE', updated_at = now() RETURNING station_prefix_id,date_hour;"
        
        await db.any(query).then(ext => {
            // console.log(ext);
            ext.forEach(line=>{
                
                STATION_LAST_MEASUREMENT[line.station_prefix_id] = STATION_LAST_MEASUREMENT[line.station_prefix_id] || line.date_hour
                if(new Date(STATION_LAST_MEASUREMENT[line.station_prefix_id]) < new Date(line.date_hour)){
                    STATION_LAST_MEASUREMENT[line.station_prefix_id] = line.date_hour
                }
            })            
            
            
            console.log(`Medições cadastradas/atualizadas: `, ext.length)
            
        }).catch(error => {
            console.log("Error Bulk Insert: ", error);
        });
    } else {
        console.log('NENHUMA MEDIÇÃO NA URL');
        
    }
    
}

const updateDateLastMeasurement = async () =>{
    console.log('Atualizando data da ultima medição');
    
    let data = Object.entries(STATION_LAST_MEASUREMENT).map(([id, date_last_measurement]) => ({id:parseInt(id),  date_last_measurement: moment(date_last_measurement, "YYYY-MM-DD HH:mm").format('YYYY-MM-DD HH:mm') }));

    // console.log(data);
    
    if(data && data.length > 0){
        console.log('Atualizando "data da ultima medição" dos postos no SIBH. Qtd: ', data.length)
        
        const cs_s = new pgp.helpers.ColumnSet(['id', {name: 'date_last_measurement',cast: "timestamp"}], { table: 'station_prefixes' });
        const query = pgp.helpers.update(data, cs_s) + ' WHERE v.id = t.id';
        
        // Executando a consulta
        await db.query(query)
        .then(() => {
            console.log('data da ultima medição dos postos no SIBH atualizada')
        })
        .catch(error => {
            console.log('Erro ao atualizar "data da ultima medição" dos postos SIBH', error);
        });
        return true
    }

    return true
    
}

const buildMeasurements = (station, list) =>{
    
    let station_type_id = station.station_type_id.toString()
    if(station && list.length > 0){
        
        measurements = []
        list.forEach(ana_measurement=>{            
            
            let date_hour = moment(ana_measurement["Data_Hora_Medicao"],'YYYY-MM-DD HH:mm')
                            
            if(process.env.enviroment === 'production'){
                date_hour.add(3, 'hours'); // vem local da API ANA
            }

            let rainfall  = ana_measurement["Chuva_Adotada"];
            let level     = ana_measurement["Cota_Adotada"];
            let discharge = ana_measurement["Vazao_Adotada"];
            
            rainfall  = sanatizeNumber(rainfall);
            level     = sanatizeNumber(level)
            discharge = sanatizeNumber(discharge)
            // return ana_measurement
            if((rainfall != null && station_type_id === '2') || (level != null && station_type_id === '1')){
                measurements.push({
                    station_prefix_id: station.id,
                    date_hour,
                    value: station_type_id === '2' ? rainfall : level,
                    read_value: station_type_id === '1' ? discharge : null,
                    measurement_classification_type_id: 3,
                    transmission_type_id:4,
                    information_origin: 'WS-ANA-NODE'
                })
            }
        })
        return measurements
    }

    return []
    
}

const sanatizeNumber = value =>{
    return ( value  != "" && value  != "null" && value != null) ? parseFloat(value)  : null
}

const tryAuthenticate = async _=>{
    console.log('Autenticando na API');
    
    try{
        let response = await authenticate()
        CURRENT_ANA_TOKEN = response.data.items.tokenautenticacao;
    } catch (e){
        CURRENT_ANA_TOKEN = ''
        console.log('Erro ao autenticar');
        return
    }
}

async function getANAStationsList(){
    return await axios.get(process.env.ANA_ENDPOINT+'/EstacoesTelemetricas/HidroInventarioEstacoes/v1',{
        params: {
            'Código da Estação': '',
            'Unidade Federativa': 'SP',
            'UF': 'SP',
            'Código da Bacia': '',
            'Data Atualização Inicial (yyyy-MM-dd)': '2000-01-01',
            'Data Atualização Final (yyyy-MM-dd)': ''
        },
        headers: {
            'accept': '*/*',
            'Authorization': `Bearer ${CURRENT_ANA_TOKEN}`
        }
    })
}

const getMeasurements = async (station, start, range) =>{
    return new Promise((resolve, error) => {
        try{
            axios_queue.get(process.env.ANA_ENDPOINT+'/EstacoesTelemetricas/HidroinfoanaSerieTelemetricaDetalhada/v1',{
                params: {
                    'Código da Estação': station.prefix,
                    'Tipo Filtro Data': 'DATA_LEITURA',
                    'Data de Busca (yyyy-MM-dd)': start,
                    'Range Intervalo de busca': range
                },
                headers: {
                    'accept': '*/*',
                    'Authorization': `Bearer ${CURRENT_ANA_TOKEN}`
                }
            }).then(response=>{
                let measurements = buildMeasurements(station, response.data?.items || [])
                MEASUREMENTS.push(...measurements)
                resolve()
            }).catch(e=>{
                resolve(e)
            })
            
        } catch (e){
            console.log(e);
            
            console.log('Erro ao buscar medições');
            console.log(station.prefix);    
            resolve(e)
        }  
    })
    
    
}

const getANASibhStations = async _ =>{
    console.log('Buscando postos ANA no SIBH');
    
    try{
        let response = await axios.get(`${process.env.SIBH_API_ENDPOINT}stations?station_owner_ids[]=1`) 
        response.data.forEach(station=>{
            SIBH_STATIONS[station.prefix] = station
        })
    } catch (e){
        SIBH_STATIONS = {}
        console.log('Erro ao consultar postos do SIBH', e);
        return
    }

}

const authenticate = async _ =>{
    return await axios.get(process.env.ANA_ENDPOINT+'/EstacoesTelemetricas/OAUth/v1',{
        headers: {
        'Identificador': process.env.ANA_USER,
        'Senha': process.env.ANA_PASSWORD,
        'accept': '*/*',
        'Authorization': `Bearer ${process.env.ANA_PASSWORD}`
        }
    })
}

