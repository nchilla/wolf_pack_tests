//this script processes the very large pub JSONs in a stream of chunks separated by month, eliminates some redundancies in the data structure, and regenerates the cleaned file.

const fs   = require('fs');

//importing a library to stream JSONs
//https://github.com/uhop/stream-json/wiki
const {chain}  = require('stream-chain');
const {parser} = require('stream-json');
const {pick}   = require('stream-json/filters/Pick');
const {streamArray} = require('stream-json/streamers/StreamArray');

//directories for data on the hard drive I'm using
//the decompressed data Noya sent us
const nico_input='/Volumes/Force/nico/4_ngram_count_JSONs/';
//a new folder to hold cleaned files
const nico_output='/Volumes/Force/nico/cleaned/';

processDirectory(nico_input,nico_output);

//function to loop through each directory and stream its contents using the dataCleaner function
async function processDirectory(inputDir,outputDir){
    let unprocessedFiles;

    //find the input directory
    try {
    unprocessedFiles = await findThis('directory', inputDir);
    } catch (e) {
    console.log(`you are missing the ${inputDir} folder`);
    }

    //filter out non-jsons
    unprocessedFiles = unprocessedFiles.filter((file) => {
        //regex to get file extension
        let fExtension = file.match(/\.[^/.]+$/)[0];
        //checks if extension exists and is .json
        return fExtension && fExtension.toLowerCase()=='.json';
    });
    
    //loops through verified jsons
    for(let file of unprocessedFiles){
        await dataCleaner(file,inputDir,outputDir);
    }

}




async function dataCleaner(file_name,inputDir,outputDir){
    return new Promise((resolve) => {
        console.log('started processing',file_name);

        //chain is another utility function by the same author as stream-json
        //https://github.com/uhop/stream-chain
        const pipeline =chain([
            fs.createReadStream(inputDir+file_name),
            parser(),
            //uses regex to find the key with the data, named using the pub name
            // e.g. "{'Des Moines Register'}"
            pick({filter: /{'(.*)'}/g}),
            streamArray(),
            data => {
                console.log('processing ',data.key);
                //gets data value
                let obj=data.value;
                //gets the single key in that object, which is the name of the month
                let key=Object.keys(obj)[0];
                //gets the data for the month (the value of that key)
                let grams=obj[key];
                //creates new object to hold restructured data
                let newGrams={};
                //looping through n=1-5 to get their corresponding arrays and add them as keys to the new object
                for(let n=1;n<6;n++){
                    let val=grams[n+' gram'];
                    newGrams[n]=val;
                }
                //if it's the first item, adding a curly bracket to start the file as an object
                let prepend=data.key==0?'{':'';
                
                //prepares data to append to new file as a json key string like so:
                // "month-name":{...data}
                return `${prepend}"${key}":${JSON.stringify(newGrams)},`;
        
            },
            //writes to file
            fs.createWriteStream(outputDir+file_name)
        ]);

        pipeline.on('end', () => {
                //when finished, close json with curly bracket and resolve promise
                fs.appendFile(outputDir+file_name, '}', function (err) {
                    if (err) throw err;
                    console.log('finished processing '+file_name);
                    resolve(true);
                });
            }
        );
    })
    
}




//helper function imported from another project that locates files and folders
function findThis(type, name) {
    return new Promise((resolve) => {
        switch (type) {
        case 'directory':
            fs.readdir(name, callback);
            break;
        case 'file':
            fs.readFile(name, 'utf8', callback);
            break;
        default:
        }

        function callback(err, data) {
        if (err) {
            resolve(undefined);
        }
        resolve(data);
        }
    });
}



