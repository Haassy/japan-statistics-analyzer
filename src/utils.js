import axios from 'axios';

const ESTAT_BASE_URL = 'https://api.e-stat.go.jp/rest/3.0/app';

const createApiClient = (appId) => {
    return axios.create({
        baseURL: ESTAT_BASE_URL,
        timeout: 30000,
        params: {
            appId: appId
        }
    });
};

const handleApiError = (error, context) => {
    if (error.response) {
        // The request was made and the server responded with a status code
        // that falls out of the range of 2xx
        const { status, data } = error.response;
        const apiErrorMsg = data?.GET_STATS_LIST?.RESULT?.ERROR_MSG || data?.GET_META_INFO?.RESULT?.ERROR_MSG || data?.GET_STATS_DATA?.RESULT?.ERROR_MSG || 'Unknown API error';
        throw new Error(`e-Stat API Error (${context}): Status ${status} - ${apiErrorMsg}`);
    } else if (error.request) {
        // The request was made but no response was received
        throw new Error(`e-Stat API Error (${context}): No response received from server.`);
    } else {
        // Something happened in setting up the request that triggered an Error
        throw new Error(`e-Stat API Error (${context}): ${error.message}`);
    }
};

export const getStatsList = async (apiClient, params = {}) => {
    try {
        const response = await apiClient.get('/getStatsList', {
            params: {
                ...params,
                lang: 'J',
                dataFormat: 'json'
            }
        });

        if (response.data?.GET_STATS_LIST?.RESULT?.STATUS === '0') {
            const tables = response.data.GET_STATS_LIST.DATALIST_INF?.TABLE_INF || [];
            return Array.isArray(tables) ? tables : [tables];
        } else {
            const errorMessage = response.data?.GET_STATS_LIST?.RESULT?.ERROR_MSG || 'Unknown error';
            throw new Error(`e-Stat API Error: ${errorMessage}`);
        }
    } catch (error) {
        handleApiError(error, 'getStatsList');
    }
};

export const getMetaInfo = async (apiClient, statsDataId) => {
    try {
        const response = await apiClient.get('/getMetaInfo', {
            params: {
                statsDataId: statsDataId,
                lang: 'J',
                dataFormat: 'json'
            }
        });

        if (response.data?.GET_META_INFO?.RESULT?.STATUS === '0') {
            return response.data.GET_META_INFO.METADATA_INF;
        } else {
            const errorMessage = response.data?.GET_META_INFO?.RESULT?.ERROR_MSG || 'Unknown error';
            throw new Error(`e-Stat API Error: ${errorMessage}`);
        }
    } catch (error) {
        handleApiError(error, `getMetaInfo for ${statsDataId}`);
    }
};

export const getStatsData = async (apiClient, statsDataId, params = {}) => {
    try {
        const response = await apiClient.get('/getStatsData', {
            params: {
                statsDataId: statsDataId,
                lang: 'J',
                dataFormat: 'json',
                ...params
            }
        });

        if (response.data?.GET_STATS_DATA?.RESULT?.STATUS === '0') {
            return response.data.GET_STATS_DATA.STATISTICAL_DATA;
        } else {
            const errorMessage = response.data?.GET_STATS_DATA?.RESULT?.ERROR_MSG || 'Unknown error';
            throw new Error(`e-Stat API Error: ${errorMessage}`);
        }
    } catch (error) {
        handleApiError(error, `getStatsData for ${statsDataId}`);
    }
};

export const normalizeStatisticalData = (rawData, metadata, sourceTableId) => {
    const normalizedData = [];
    
    try {
        const tableInf = rawData.TABLE_INF;
        const classInf = rawData.CLASS_INF;
        const dataInf = rawData.DATA_INF;

        const statName = tableInf?.TITLE || 'Unknown Statistic';
        const surveyDate = tableInf?.SURVEY_DATE || 'Unknown Date';
        
        const classObj = {};
        if (classInf?.CLASS_OBJ) {
            const classes = Array.isArray(classInf.CLASS_OBJ) ? classInf.CLASS_OBJ : [classInf.CLASS_OBJ];
            classes.forEach(cls => {
                if (cls.CLASS) {
                    const classItems = Array.isArray(cls.CLASS) ? cls.CLASS : [cls.CLASS];
                    classObj[cls['@id']] = classItems.reduce((acc, item) => {
                        acc[item['@code']] = item['@name'];
                        return acc;
                    }, {});
                }
            });
        }

        if (dataInf?.VALUE) {
            const values = Array.isArray(dataInf.VALUE) ? dataInf.VALUE : [dataInf.VALUE];
            
            values.forEach(valueItem => {
                const value = parseFloat(valueItem['$']) || 0;
                const unit = valueItem['@unit'] || '';
                
                const categories = {};
                Object.keys(valueItem).forEach(key => {
                    if (key.startsWith('@') && key !== '@unit') {
                        const classId = key.substring(1);
                        const code = valueItem[key];
                        categories[classId] = classObj[classId]?.[code] || code;
                    }
                });

                normalizedData.push({
                    statName,
                    surveyDate,
                    region: categories.area || categories.region || 'Japan',
                    category1: categories.cat01 || categories.tab || 'General',
                    category2: categories.cat02 || categories.cat03 || '',
                    value,
                    unit,
                    sourceTableId,
                    dataType: determineDataType(statName),
                    lastUpdated: tableInf?.UPDATED_DATE || new Date().toISOString(),
                    metadata: {
                        tableTitle: statName,
                        categories: categories,
                        originalAttributes: valueItem
                    },
                    extractedAt: new Date().toISOString()
                });
            });
        }
    } catch (error) {
        console.error('Error normalizing data:', error.message);
        normalizedData.push({
            statName: rawData.TABLE_INF?.TITLE || 'Unknown Statistic',
            surveyDate: rawData.TABLE_INF?.SURVEY_DATE || 'Unknown Date',
            region: 'Japan',
            category1: 'General',
            category2: '',
            value: 0,
            unit: '',
            sourceTableId,
            dataType: 'unknown',
            lastUpdated: new Date().toISOString(),
            metadata: { error: error.message, rawData },
            extractedAt: new Date().toISOString()
        });
    }

    return normalizedData;
};

const determineDataType = (statName) => {
    const name = statName.toLowerCase();
    
    if (name.includes('人口') || name.includes('population')) return 'population';
    if (name.includes('経済') || name.includes('gdp') || name.includes('経済')) return 'economic';
    if (name.includes('労働') || name.includes('雇用') || name.includes('labor')) return 'labor';
    if (name.includes('産業') || name.includes('industry')) return 'industry';
    if (name.includes('教育') || name.includes('education')) return 'education';
    if (name.includes('医療') || name.includes('health')) return 'health';
    if (name.includes('環境') || name.includes('environment')) return 'environment';
    
    return 'general';
};

export const validateInput = (input) => {
    const validated = {
        searchKeyword: input.searchKeyword?.trim() || '',
        surveyYears: input.surveyYears?.trim() || '',
        statsField: input.statsField?.trim() || '',
        maxItems: Math.min(Math.max(parseInt(input.maxItems) || 10, 1), 100),
        includeMetadata: Boolean(input.includeMetadata !== false),
        outputFormat: ['structured', 'raw', 'both'].includes(input.outputFormat) ? input.outputFormat : 'structured'
    };

    return validated;
};

export const createEstatApiClient = (appId) => {
    if (!appId) {
        throw new Error('e-Stat API Application ID is required. Please set ESTAT_APP_ID environment variable.');
    }
    
    return createApiClient(appId);
};

export const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

export const logProgress = (message, data = {}) => {
    console.log(`[${new Date().toISOString()}] ${message}`, data);
};




/**
 * Log a monitoring event.
 * @param {string} eventName - The name of the event.
 * @param {Object} data - Additional data associated with the event.
 */
export const logEvent = (eventName, data = {}) => {
    const event = {
        event: eventName,
        timestamp: new Date().toISOString(),
        ...data,
    };
    console.log(`[MONITORING] ${JSON.stringify(event)}`);
};

