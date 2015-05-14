// Adapted from https://github.com/xehrad/UUID_to_Date
var UUID_to_Date = function(uuid_str) {
    var GREGORIAN_OFFSET = 122192928000000000;
    var get_time_int = function (uuid_str) {
	// (string) uuid_str format	=>		'11111111-2222-#333-4444-555555555555'
	var uuid_arr = uuid_str.split( '-' ),
	    time_str = [
		uuid_arr[ 2 ].substring( 1 ),
		uuid_arr[ 1 ],
		uuid_arr[ 0 ]
	    ].join( '' );
	// time_str is convert  '11111111-2222-#333-4444-555555555555'  to  '333222211111111'
	return parseInt( time_str, 16 );
    };
    // (string) uuid_str format	=>		'11111111-2222-#333-4444-555555555555'
    var int_time = get_time_int( uuid_str ) - GREGORIAN_OFFSET,
	int_millisec = Math.floor( int_time / 10000 );
    return new Date( int_millisec );
}
