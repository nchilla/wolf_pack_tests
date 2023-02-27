
// TO COME BACK TO: the end event for the pipeline doesn't trigger for some reason

const fs   = require('fs');

//importing a library to stream JSONs
//https://github.com/uhop/stream-json/wiki
const {chain}  = require('stream-chain');
const {parser} = require('stream-json');
const {streamObject} = require('stream-json/streamers/StreamObject');


//directory for the (cleaned) JSONs on the hard drive I'm using
const nico_input='/Volumes/chilla/nico/cleaned/';
//place to save json of totals
const nico_output='/Volumes/chilla/nico/';

//start a json for the total occurrences and months repeated for each term across the database
const totals={
    "1":{},
    "2":{},
    "3":{},
    "4":{},
    "5":{}
};

testCount(nico_input,nico_output);

async function testCount(inputDir,outputDir){
  await processPubTotals('des-moines-register.json',inputDir);
  saveTotals(nico_output)
}


async function processPubTotals(file_name,inputDir){
  return new Promise((resolve) => {
    console.log('started processing',file_name);

    //chain is another utility function by the same author as stream-json
    //https://github.com/uhop/stream-chain
    const pipeline =chain([
        fs.createReadStream(inputDir+file_name),
        parser(),
        streamObject(),
        data => {
          for(let n=1;n<6;n++){
            let grams=data.value[n];
            for (const [gram, count] of Object.entries(grams)) {
                if(totals[n][gram]){
                  // delete totals[n][gram]
                  totals[n][gram].c+=count;
                  totals[n][gram].r++;
                }else{
                  totals[n][gram]={
                    c:count,
                    r:1
                  }
                }
            }
            
          }
        }
    ]);

    pipeline.on('finish', () => {
          console.log('finished I hope')
          resolve(true);
      }
    );

  })
}

function saveTotals(outputDir){
    //save totals to 
    fs.writeFile(outputDir+'totals.json', JSON.stringify(totals), err => {
        if (err) {
          console.error(err)
          return
        }
      })
}

//{
// "1":{
//     "dollar":{
//          "occurrences"
//          "months repeated"
//      }
// }
//}

//loop through the entire database
    // for each month> for each n-gram count> for each term, check if it's already saved in the totaler. If yes, add the month count to the total for that term
    //maybe also separately add 1 to a count of months that it occurred in?