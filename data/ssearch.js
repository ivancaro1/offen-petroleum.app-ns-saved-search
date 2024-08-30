var
    log,
    search,
    response = new Object();

define(['N/log', 'N/search'], main);

function main(logModule, searchModule) {
    log = logModule;
    search = searchModule;

    return { post: postProcess }
}

function postProcess(request) {
    // Log the incoming request for debugging purposes
    log.debug({ title: 'Incoming Request', details: JSON.stringify(request) });

    try {
        // Check if searchID is provided
        if ((typeof request.searchID == 'undefined') || (request.searchID === null) || (request.searchID == '')) {
            throw { 'type': 'error.SavedSearchAPIError', 'name': 'INVALID_REQUEST', 'message': 'No searchID was specified.' };
        }

        // Load the saved search
        var searchObj = search.load({ id: request.searchID });
        response.results = [];

        var resultSet = searchObj.run();
        var start = 0;
        var results = [];

        // Fetch results in batches of 1000
        do {
            results = resultSet.getRange({ start: start, end: start + 1000 });
            start += 1000;
            response.results = response.results.concat(results);
        } while (results.length);

        return response;

    } catch (e) {
        // Enhanced error logging
        log.debug({ title: 'Error Occurred', details: e });
        return { 'error': { 'type': e.type || 'error', 'name': e.name || 'Unknown Error', 'message': e.message || 'An unknown error occurred.' } };
    }
}
