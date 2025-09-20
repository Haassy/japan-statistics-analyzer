import { jest } from '@jest/globals';

describe('Utility Functions', () => {

  it('getStatsList should call axios.get with correct parameters and return data', async () => {
    await jest.isolateModulesAsync(async () => {
      const mockAxiosGet = jest.fn().mockResolvedValue({
        data: { 'GET_STATS_LIST': { 'RESULT': { 'STATUS': '0' }, 'DATALIST_INF': { 'TABLE_INF': [{ id: 'test-table' }] } } }
      });

      jest.doMock('axios', () => ({
        __esModule: true,
        default: {
          create: () => ({ get: mockAxiosGet }),
        },
      }));

      const { createEstatApiClient, getStatsList } = await import('../src/utils.js');
      const apiClient = createEstatApiClient('test-app-id');
      const result = await getStatsList(apiClient, { searchWord: 'test' });

      expect(mockAxiosGet).toHaveBeenCalledTimes(1);
      expect(mockAxiosGet).toHaveBeenCalledWith('/getStatsList', {
        params: {
          searchWord: 'test',
          lang: 'J',
          dataFormat: 'json'
        }
      });
      expect(result).toEqual([{ id: 'test-table' }]);
    });
  });

  it('getMetaInfo should call axios.get with correct parameters and return metadata', async () => {
    await jest.isolateModulesAsync(async () => {
      const mockAxiosGet = jest.fn().mockResolvedValue({
        data: { 'GET_META_INFO': { 'RESULT': { 'STATUS': '0' }, 'METADATA_INF': { tableName: 'test-meta' } } }
      });

      jest.doMock('axios', () => ({
        __esModule: true,
        default: {
          create: () => ({ get: mockAxiosGet }),
        },
      }));

      const { createEstatApiClient, getMetaInfo } = await import('../src/utils.js');
      const apiClient = createEstatApiClient('test-app-id');
      const result = await getMetaInfo(apiClient, 'test-stats-id');

      expect(mockAxiosGet).toHaveBeenCalledTimes(1);
      expect(mockAxiosGet).toHaveBeenCalledWith('/getMetaInfo', {
        params: {
          statsDataId: 'test-stats-id',
          lang: 'J',
          dataFormat: 'json'
        }
      });
      expect(result).toEqual({ tableName: 'test-meta' });
    });
  });

  it('getStatsData should call axios.get with correct parameters and return statistical data', async () => {
    await jest.isolateModulesAsync(async () => {
      const mockAxiosGet = jest.fn().mockResolvedValue({
        data: { 'GET_STATS_DATA': { 'RESULT': { 'STATUS': '0' }, 'STATISTICAL_DATA': { data: 'test-data' } } }
      });

      jest.doMock('axios', () => ({
        __esModule: true,
        default: {
          create: () => ({ get: mockAxiosGet }),
        },
      }));

      const { createEstatApiClient, getStatsData } = await import('../src/utils.js');
      const apiClient = createEstatApiClient('test-app-id');
      const result = await getStatsData(apiClient, 'test-stats-id');

      expect(mockAxiosGet).toHaveBeenCalledTimes(1);
      expect(mockAxiosGet).toHaveBeenCalledWith('/getStatsData', {
        params: {
          statsDataId: 'test-stats-id',
          lang: 'J',
          dataFormat: 'json'
        }
      });
      expect(result).toEqual({ data: 'test-data' });
    });
  });

  it('validateInput should return validated and default values', async () => {
    // This test doesn't require mocks, so isolation is not strictly necessary but good practice.
    await jest.isolateModulesAsync(async () => {
      const { validateInput } = await import('../src/utils.js');
      const input = { searchKeyword: '  test  ', maxItems: '200', outputFormat: 'invalid' };
      const validated = validateInput(input);

      expect(validated.searchKeyword).toBe('test');
      expect(validated.maxItems).toBe(100);
      expect(validated.outputFormat).toBe('structured');
      expect(validated.includeMetadata).toBe(true);
    });
  });
});



  it('getStatsList should throw a formatted error on API error response', async () => {
    await jest.isolateModulesAsync(async () => {
      const mockAxiosGet = jest.fn().mockResolvedValue({
        data: { 'GET_STATS_LIST': { 'RESULT': { 'STATUS': '1', 'ERROR_MSG': 'Test API Error' } } }
      });

      jest.doMock('axios', () => ({
        __esModule: true,
        default: {
          create: () => ({ get: mockAxiosGet }),
        },
      }));

      const { createEstatApiClient, getStatsList } = await import('../src/utils.js');
      const apiClient = createEstatApiClient('test-app-id');

      await expect(getStatsList(apiClient, { searchWord: 'test' })).rejects.toThrow('e-Stat API Error: Test API Error');
    });
  });

  it('getStatsList should throw a formatted error on network error', async () => {
    await jest.isolateModulesAsync(async () => {
      const mockAxiosGet = jest.fn().mockRejectedValue(new Error('Network Error'));

      jest.doMock('axios', () => ({
        __esModule: true,
        default: {
          create: () => ({ get: mockAxiosGet }),
        },
      }));

      const { createEstatApiClient, getStatsList } = await import('../src/utils.js');
      const apiClient = createEstatApiClient('test-app-id');

      await expect(getStatsList(apiClient, { searchWord: 'test' })).rejects.toThrow('e-Stat API Error (getStatsList): Network Error');
    });
  });
