import parseArgs from 'minimist'
import FormData from 'form-data'
import axios from 'axios'
import fs from 'fs'

// npm run start -- --api=http://localhost:3001 --user=user --pass=pass pid1 pid2 ...

const argv = parseArgs(process.argv.slice(2))

const oefos = JSON.parse(fs.readFileSync('./oefos.json', 'utf8'));

console.debug('args:')
console.dir(argv)

// if (argv._.length < 1) {
//   console.error('Missing pids');
//   process.exit(1)
// }

if (!argv.api) {
  console.error('Missing api param');
  process.exit(1)
}

if (!argv.user) {
  console.error('Missing user param');
  process.exit(1)
}

if (!argv.pass) {
  console.error('Missing pass param');
  process.exit(1)
}

const pids = [
  "o:1643160",
    
  "o:1639909",
      
  "o:1639054",
      
  "o:1639052",
      
  "o:1639051",
      
  "o:1639050",
      
  "o:1639048",
      
  "o:1639036",
      
  "o:1639025",
      
  "o:1639024",
      
  "o:1639023",
      
  "o:1639010",
      
  "o:1639009",
      
  "o:1639008",
      
  "o:1638985",
      
  "o:1638984",
      
  "o:1638983",
      
  "o:1638982",
      
  "o:1638981",
      
  "o:1638980",
      
  "o:1638978",
      
  "o:1638977",
      
  "o:1638976",
      
  "o:1638000",
      
  "o:1637999",
      
  "o:1637998",
      
  "o:1637997",
      
  "o:1637996",
      
  "o:1637995",
      
  "o:1637994",
      
  "o:1637993",
      
  "o:1637992",
      
  "o:1637991",
      
  "o:1637990",
      
  "o:1637953",
      
  "o:1637952",
      
  "o:1637951",
      
  "o:1637950",
      
  "o:1637949",
      
  "o:1637948",
      
  "o:1637946",
      
  "o:1637945",
      
  "o:1637777",
      
  "o:1637776",
      
  "o:1637775",
      
  "o:1637774",
      
  "o:1637773",
      
  "o:1637766",
      
  "o:1637699",
      
  "o:1631090"
]

async function getMetadata(pid) {
  try {
    const response = await axios.get(argv.api + '/object/' + pid + '/metadata', {
      auth: {
        username: argv.user,
        password: argv.pass
      }
    })
    if (response.data.alerts) {
      if (response.data.alerts.length > 0) {
        console.warn(response.data.alerts)
      }
    }
    if (response.data.metadata) {
      return response.data.metadata
    }
  } catch (error) {
    if (error.response) {
      console.error(error.response.data)
      console.error(error.response.status)
      console.error(error.response.headers)
    } else if (error.request) {
      console.error(error.request)
    } else {
      console.error('Error', error.message)
    }
    console.error('fetching metadata failed pid[' + pid + ']')
    return
  }
}

async function saveMetadata(pid, metadata) {
  var httpFormData = new FormData()
  httpFormData.append('metadata', metadata)

  try {
    const response = await axios.post(argv.api + '/object/' + pid + '/metadata', httpFormData, {
      headers: httpFormData.getHeaders(),
      auth: {
        username: argv.user,
        password: argv.pass
      }
    })
    if (response.data.alerts) {
      if (response.data.alerts.length > 0) {
        console.warn(response.data.alerts)
      }
    }
    return 1
  } catch (error) {
    if (error.response) {
      console.log('pid [' + pid + '] error: ' + error.response.status)
      if (error.response.data.alerts) {
        if (error.response.data.alerts.length > 0) {
          console.warn(error.response.data.alerts)
        }
      }
    } else if (error.request) {
      console.log('pid [' + pid + '] error')
    } else {
      console.log('pid [' + pid + '] error ' + error.message)
    }
    return 0
  }
}

function getTerm (id) {
  for (let i = 0; i < oefos.terms.length; i++) {
    if (oefos.terms[i]['@id'] === id) {
      return oefos.terms[i]
    }
  }
}

function getOefosPath (term, children, path) {
  if (term) {
    for (let t of children) {
      if (t['@id'] === term['@id']) {
        path.push(t)
        return true
      } else {
        if (t.hasOwnProperty('children')) {
          if (Array.isArray(t.children)) {
            if (t.children.length > 0) {
              if (getOefosPath(term, t.children, path)) {
                path.push(t)
                return true
              }
            }
          }
        }
      }
    }
  }
}

async function processMetadata(metadata) {
  const jsonld = metadata["JSON-LD"]
  
  if (jsonld['dcterms:subject']) {
    for (let sub of jsonld['dcterms:subject']) {
      for (let em of sub['skos:exactMatch']) {
        if (em.startsWith('oefos2012:')) {
          const term = getTerm(em)
          let pathArr = []
          let pathLabelsDeu = []
          let pathLabelsEng = []
          getOefosPath(term, oefos.tree, pathArr)
          for (let i = pathArr.length; i--; i === 0) {
            pathLabelsDeu.push(pathArr[i]['skos:prefLabel']['deu'] + ' (' + pathArr[i]['skos:notation'][0] + ')')
            pathLabelsEng.push(pathArr[i]['skos:prefLabel']['eng'] + ' (' + pathArr[i]['skos:notation'][0] + ')')
          }
          sub['rdfs:label'] = [
            {
              '@language': 'deu',
              '@value': 'ÖFOS 2012 -- ' + pathLabelsDeu.join(' -- ')
            },
            {
              '@language': 'eng',
              '@value': 'ÖFOS 2012 -- ' + pathLabelsEng.join(' -- ')
            }
          ]
          break
        }
      }
    }
  }

  return JSON.stringify({ 'metadata': { 'json-ld': jsonld } })
}

async function processPids() {
  let i = 0
  for (const pid of pids) {
    i++
    const metadata = await getMetadata(pid)
    if (metadata) {
      const metadataNew = await processMetadata(metadata)
      if(await saveMetadata(pid, metadataNew)) {
        console.log('\n[' + i + '/' + pids.length + '] pid[' + pid + '] success')
      } else {
        console.log('\n[' + i + '/' + pids.length + '] pid[' + pid + '] failed')
      }
    }
  }
}

processPids()
