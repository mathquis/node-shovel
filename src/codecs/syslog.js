module.exports = () => {
	const parsers = [
    // RFC5424
		/^<(?<pri>[0-9]+)>(?<version>[0-9]+) (?<timestamp>[0-9-:.TZ]+) (?<hostname>[^\s]+) (?<identity>[^\s]+) (?<pid>[^\s]+) (?<msgid>[^\s]+) (?<data>[^\s]+) (?<text>.+)$/gu,
    // RFC3164
		/^<(?<pri>[0-9]+)>(?<timestamp>[A-Za-z]+ [0-9]+ [0-9:]+) (?<hostname>[^\s]+) (?<text>.+)$/gu
	]

  const structuredDataRegex = /([^ ]+)="([^"]+)"/gu

	const severities = [
		'emergency',
		'alert',
		'critical',
		'error',
		'warning',
		'notice',
		'informational',
		'debug'
	]

	const facilities = [
		'kernel messages',
		'user-level messages',
		'mail system',
		'system daemons',
		'security/authorization messages (note 1)',
		'messages generated internally by syslogd',
		'line printer subsystem',
		'network news subsystem',
		'UUCP subsystem',
		'clock daemon (note 2)',
		'security/authorization messages (note 1)',
		'FTP daemon',
		'NTP subsystem',
		'log audit (note 1)',
		'log alert (note 1)',
		'clock daemon (note 2)',
		'local use 0  (local0)',
		'local use 1  (local1)',
		'local use 2  (local2)',
		'local use 3  (local3)',
		'local use 4  (local4)',
		'local use 5  (local5)',
		'local use 6  (local6)',
		'local use 7  (local7)'
	]

	return {
		decode: async log => {
			const source = log.toString('utf8').trim()

      let match
      const len = parsers.length
      for ( let i = 0 ; i < len ; i++ ) {
        parsers[i].lastIndex = 0
        match = parsers[i].exec(source)
        if ( match !== null ) {
          break
        }
      }
			if ( !match ) throw new Error(`Unable to parse syslog format: ${log}`)

      const {pri, version, timestamp, hostname, identity, pid, msgid, data, text} = match.groups

      let priority = parseInt(pri)
      if ( isNaN(priority) ) {
         priority = 0
      }

      const extractedData = ( data || '' ).matchAll(structuredDataRegex)
			let structuredData = {}
			if ( extractedData ) {
				for ( i in extractedData ) {
					structuredData[extractedData[i][1]] = extractedData[i][2]
				}
			}

      const facility = Math.floor(priority / severities.length)

      const severity = priority - facility * 8

      return {
				source,
				priority,
				facility,
				facility_name: facilities[facility] || '',
				severity,
				severity_name: severities[severity] || '',
				version,
				timestamp,
				hostname,
				identity,
				pid,
				msgid,
				data: structuredData,
				message: text
			}
		}
	}
}