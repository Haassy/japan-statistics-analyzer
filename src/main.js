import { Actor } from 'apify';
import { 
    createEstatApiClient, 
    getStatsList, 
    getMetaInfo, 
    getStatsData, 
    normalizeStatisticalData,
    validateInput,
    sleep,
    logProgress,
    logEvent
} from './utils.js';

export async function mainLogic() {
    logEvent('actor.started');
    // Get input parameters
    const input = await Actor.getInput();
    logProgress('Actor started', { input });

    // Validate input parameters
    const validatedInput = validateInput(input || {});
    logProgress('Input validated', validatedInput);

    // Get e-Stat API credentials
    const estatAppId = process.env.ESTAT_APP_ID || await Actor.getValue('ESTAT_APP_ID');
    
    if (!estatAppId) {
        // Demo mode - provide sample data when no API key is available
        logEvent('actor.demo_mode');
        logProgress('Running in demo mode - no e-Stat API key provided');
        await runDemoMode(validatedInput);
        logEvent('actor.completed', { mode: 'demo' });
        return;
    }

    try {
        // Create API client
        const apiClient = createEstatApiClient(estatAppId);
        logProgress('e-Stat API client created');

        // Build search parameters
        const searchParams = buildSearchParams(validatedInput);
        logProgress('Search parameters built', searchParams);

        // Get list of statistical tables
        logEvent('api.call_initiated', { api: 'getStatsList', params: searchParams });
        logProgress('Fetching statistical tables list...');
        const statsList = await getStatsList(apiClient, searchParams);
        logEvent('api.call_successful', { api: 'getStatsList', resultCount: statsList.length });
        logProgress(`Found ${statsList.length} statistical tables`);

        if (statsList.length === 0) {
            logProgress('No statistical tables found for the given criteria');
            await Actor.pushData({
                message: 'No statistical tables found for the given search criteria',
                searchParams: searchParams,
                extractedAt: new Date().toISOString()
            });
            logEvent('actor.completed', { mode: 'api', status: 'no_data' });
            return;
        }

        // Process each statistical table (up to maxItems)
        const tablesToProcess = statsList.slice(0, validatedInput.maxItems);
        logProgress(`Processing ${tablesToProcess.length} tables`);

        let totalDataPoints = 0;
        let processedTables = 0;

        for (const table of tablesToProcess) {
            try {
                processedTables++;
                logEvent('table.processing_started', { tableId: table['@id'], title: table.TITLE });
                logProgress(`Processing table ${processedTables}/${tablesToProcess.length}: ${table.TITLE}`, {
                    tableId: table['@id'],
                    title: table.TITLE
                });

                // Get metadata if requested
                let metadata = null;
                if (validatedInput.includeMetadata) {
                    try {
                        logEvent('api.call_initiated', { api: 'getMetaInfo', tableId: table['@id'] });
                        metadata = await getMetaInfo(apiClient, table['@id']);
                        logEvent('api.call_successful', { api: 'getMetaInfo', tableId: table['@id'] });
                        logProgress('Metadata retrieved');
                    } catch (metaError) {
                        logEvent('api.call_failed', { api: 'getMetaInfo', tableId: table['@id'], error: metaError.message });
                        logProgress('Warning: Could not retrieve metadata', { error: metaError.message });
                    }
                }

                // Get statistical data
                logEvent('api.call_initiated', { api: 'getStatsData', tableId: table['@id'] });
                const statsData = await getStatsData(apiClient, table['@id']);
                logEvent('api.call_successful', { api: 'getStatsData', tableId: table['@id'] });
                logProgress('Statistical data retrieved');

                // Process output format
                if (validatedInput.outputFormat === 'raw' || validatedInput.outputFormat === 'both') {
                    await Actor.pushData({
                        type: 'raw',
                        tableId: table['@id'],
                        tableInfo: table,
                        metadata: metadata,
                        rawData: statsData,
                        extractedAt: new Date().toISOString()
                    });
                }

                if (validatedInput.outputFormat === 'structured' || validatedInput.outputFormat === 'both') {
                    const normalizedData = normalizeStatisticalData(statsData, metadata, table['@id']);
                    for (const dataPoint of normalizedData) {
                        await Actor.pushData(dataPoint);
                        totalDataPoints++;
                    }
                    logProgress(`Processed ${normalizedData.length} data points from table`);
                }

                logEvent('table.processing_successful', { tableId: table['@id'] });
                // Rate limiting - wait 1 second between requests
                await sleep(validatedInput.delayBetweenRequests);

            } catch (tableError) {
                logEvent('table.processing_failed', { tableId: table['@id'], error: tableError.message });
                logProgress(`Error processing table ${table['@id']}`, { 
                    error: tableError.message,
                    tableTitle: table.TITLE 
                });
                
                await Actor.pushData({
                    type: 'error',
                    tableId: table['@id'],
                    tableTitle: table.TITLE,
                    error: tableError.message,
                    extractedAt: new Date().toISOString()
                });
            }
        }

        // Final summary
        logProgress('Processing completed', {
            tablesProcessed: processedTables,
            totalDataPoints: totalDataPoints,
            searchCriteria: validatedInput
        });

        await Actor.pushData({
            type: 'summary',
            tablesProcessed: processedTables,
            totalDataPoints: totalDataPoints,
            searchCriteria: validatedInput,
            completedAt: new Date().toISOString()
        });
        logEvent('actor.completed', { mode: 'api', status: 'success', tablesProcessed: processedTables, totalDataPoints });

    } catch (error) {
        logEvent('api.call_failed', { error: error.message });
        logProgress('API error occurred, falling back to demo mode', { error: error.message });
        
        if (error.message.includes('403') || error.message.includes('401') || error.message.includes('API')) {
            logProgress('Falling back to demo mode due to API authentication issues');
            await runDemoMode(validatedInput);
            logEvent('actor.completed', { mode: 'demo', status: 'fallback' });
            return;
        }
        
        await Actor.pushData({
            type: 'error',
            error: error.message,
            message: 'An error occurred while processing. Falling back to demo mode.',
            input: validatedInput,
            extractedAt: new Date().toISOString()
        });
        
        await runDemoMode(validatedInput);
        logEvent('actor.completed', { mode: 'demo', status: 'error_fallback' });
    }
}

function buildSearchParams(input) {
    const params = {};
    if (input.searchKeyword) params.searchWord = input.searchKeyword;
    if (input.surveyYears) params.surveyYears = input.surveyYears;
    if (input.statsField) params.statsField = input.statsField;
    params.limit = Math.min(input.maxItems * 2, 100);
    return params;
}

async function runDemoMode(input) {
    logProgress('Generating demo data for Japan Government Statistics Analyzer');
    const demoData = [
        {
            statName: '国勢調査 人口総数',
            surveyDate: '2020年',
            region: '全国',
            category1: '総人口',
            category2: '男女計',
            value: 125836021,
            unit: '人',
            sourceTableId: 'demo_001',
            dataType: 'population',
            lastUpdated: '2021-06-25T00:00:00Z',
            metadata: { tableTitle: '国勢調査 人口総数', categories: { area: '全国', gender: '男女計' }, note: 'This is demo data' },
            extractedAt: new Date().toISOString()
        },
        {
            statName: '労働力調査 就業者数',
            surveyDate: '2023年12月',
            region: '全国',
            category1: '就業者',
            category2: '総数',
            value: 67230000,
            unit: '人',
            sourceTableId: 'demo_002',
            dataType: 'labor',
            lastUpdated: '2024-01-30T00:00:00Z',
            metadata: { tableTitle: '労働力調査 就業者数', categories: { area: '全国', employment: '就業者' }, note: 'This is demo data' },
            extractedAt: new Date().toISOString()
        },
    ];

    let filteredData = demoData;
    if (input.searchKeyword) {
        const keyword = input.searchKeyword.toLowerCase();
        filteredData = filteredData.filter(item => 
            item.statName.toLowerCase().includes(keyword) ||
            item.category1.toLowerCase().includes(keyword) ||
            item.dataType.toLowerCase().includes(keyword)
        );
    }
    filteredData = filteredData.slice(0, input.maxItems);

    for (const dataPoint of filteredData) {
        await Actor.pushData(dataPoint);
    }

    await Actor.pushData({
        type: 'demo_summary',
        message: 'Demo mode completed. To access real e-Stat data, please provide ESTAT_APP_ID.',
        demoDataPoints: filteredData.length,
        searchCriteria: input,
        registrationInfo: { url: 'https://www.e-stat.go.jp/api/', note: 'Register for free e-Stat API access' },
        completedAt: new Date().toISOString()
    });

    logProgress('Demo mode completed', { dataPoints: filteredData.length, searchKeyword: input.searchKeyword });
}

if (process.env.NODE_ENV !== 'test') {
    Actor.main(mainLogic);
}

