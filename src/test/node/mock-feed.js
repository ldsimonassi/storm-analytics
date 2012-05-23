var http = require('http');
var parser = require('url');
var os = require('os');
var fs = require('fs');

var sequence = [];
var counter = 0;

function load_sequence(seq_file) {
	var content = fs.readFileSync(seq_file);
	sequence = JSON.parse(content);
	var val = JSON.stringify(sequence);
	console.log(val);
}


var seq_file;

if(process.argv.length>2) {
    seq_file = process.argv[2];
} else {
	seq_file = "./sequence.mock";
	console.log("No argument provided, using ./sequence.mock");
}

load_sequence(seq_file);

function calculate_index(i) {
	return i%sequence.length;
}

// Server to be used for requesting pending tasks
http.createServer(function (request, response) {
	var parsed_url= parser.parse(request.url, true);
	var query = request.url;

	if(query=="/isAlive")
		response.end("YES!");
	else {
		var max= parsed_url.query.max;
		if(max==null || typeof(max)=="undefined")
			max=1;

		console.log("Reading max:" + max);
		response.writeHead(200, {'Content-Type': 'text/json'});

		var resp = [];
		// Build the response array.
		for(var i = 0; i < max; i++) {
			var idx = calculate_index(i+counter);
			resp[i] = sequence[idx];
		}

		var content = JSON.stringify(resp);
		console.log(content);
		response.write(content);

		response.end();
	}
}).listen(8080);

