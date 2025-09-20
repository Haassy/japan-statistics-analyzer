import { jest } from '@jest/globals';

describe('Japan Statistics Analyzer - Main Logic', () => {
  // A helper function to create a complete mock for the utils module
  const getUtilsMock = () => ({
    createEstatApiClient: jest.fn(() => ({})),
    getStatsList: jest.fn().mockResolvedValue([]),
    getMetaInfo: jest.fn().mockResolvedValue({}),
    getStatsData: jest.fn().mockResolvedValue({}),
    normalizeStatisticalData: jest.fn(() => []),
    validateInput: jest.fn((input) => ({
      maxItems: 10,
      includeMetadata: true,
      outputFormat: 'structured',
      delayBetweenRequests: 1000, // Default value
      ...input, // The actual input will override the default
    })),
    sleep: jest.fn().mockResolvedValue(undefined),
    logProgress: jest.fn(),
    logEvent: jest.fn(),
  });

  it('should run in demo mode when no API key is provided', async () => {
    await jest.isolateModulesAsync(async () => {
      // --- 1. Mocks ---
      const mockGetInput = jest.fn().mockResolvedValue({ searchKeyword: 'test' });
      const mockGetValue = jest.fn().mockResolvedValue(null); // No API key
      const mockPushData = jest.fn();
      const utilsMock = getUtilsMock();

      jest.doMock('apify', () => ({
        Actor: {
          main: jest.fn(async (fn) => fn()),
          getInput: mockGetInput,
          getValue: mockGetValue,
          pushData: mockPushData,
        },
      }));

      jest.doMock('../src/utils.js', () => utilsMock);

      // --- 2. Execution ---
      const { mainLogic } = await import('../src/main.js');
      await mainLogic();

      // --- 3. Assertions ---
      expect(utilsMock.logEvent).toHaveBeenCalledWith('actor.started');
      expect(utilsMock.logEvent).toHaveBeenCalledWith('actor.demo_mode');
      expect(mockGetInput).toHaveBeenCalledTimes(1);
      expect(mockGetValue).toHaveBeenCalledWith('ESTAT_APP_ID');
      expect(utilsMock.getStatsList).not.toHaveBeenCalled();
      expect(utilsMock.logEvent).toHaveBeenCalledWith('actor.completed', { mode: 'demo' });
    });
  });

  it('should call the e-Stat API when an API key is provided', async () => {
    await jest.isolateModulesAsync(async () => {
      // --- 1. Mocks ---
      const mockGetInput = jest.fn().mockResolvedValue({ maxItems: 5 });
      const mockGetValue = jest.fn().mockResolvedValue('test-app-id');
      const mockPushData = jest.fn();
      const utilsMock = getUtilsMock();

      jest.doMock('apify', () => ({
        Actor: {
          main: jest.fn(async (fn) => fn()),
          getInput: mockGetInput,
          getValue: mockGetValue,
          pushData: mockPushData,
        },
      }));

      jest.doMock('../src/utils.js', () => utilsMock);

      // --- 2. Execution ---
      const { mainLogic } = await import('../src/main.js');
      await mainLogic();

      // --- 3. Assertions ---
      expect(utilsMock.logEvent).toHaveBeenCalledWith('actor.started');
      expect(mockGetValue).toHaveBeenCalledWith('ESTAT_APP_ID');
      expect(utilsMock.createEstatApiClient).toHaveBeenCalledWith('test-app-id');
      expect(utilsMock.getStatsList).toHaveBeenCalledTimes(1);
      expect(utilsMock.logEvent).toHaveBeenCalledWith('actor.completed', expect.objectContaining({ mode: 'api' }));
    });
  });

  it('should log a failure event when the API call fails', async () => {
    await jest.isolateModulesAsync(async () => {
        // --- 1. Mocks ---
        const mockGetInput = jest.fn().mockResolvedValue({ maxItems: 5 });
        const mockGetValue = jest.fn().mockResolvedValue('test-app-id');
        const utilsMock = getUtilsMock();
        utilsMock.getStatsList.mockRejectedValue(new Error('API Failure'));

        jest.doMock('apify', () => ({
            Actor: {
                main: jest.fn(async (fn) => fn()),
                getInput: mockGetInput,
                getValue: mockGetValue,
                pushData: jest.fn(), // Mock pushData to avoid errors on fallback
            },
        }));

        jest.doMock('../src/utils.js', () => utilsMock);

        // --- 2. Execution ---
        const { mainLogic } = await import('../src/main.js');
        await mainLogic();

        // --- 3. Assertions ---
        expect(utilsMock.logEvent).toHaveBeenCalledWith('actor.started');
        expect(utilsMock.getStatsList).toHaveBeenCalledTimes(1);
        expect(utilsMock.logEvent).toHaveBeenCalledWith('api.call_failed', { error: 'API Failure' });
        expect(utilsMock.logEvent).toHaveBeenCalledWith('actor.completed', expect.objectContaining({ status: 'fallback' }));
    });
  });

  it('should use the specified delay between requests', async () => {
    await jest.isolateModulesAsync(async () => {
        // --- 1. Mocks ---
        const mockGetInput = jest.fn().mockResolvedValue({ maxItems: 1, delayBetweenRequests: 1500 });
        const mockGetValue = jest.fn().mockResolvedValue('test-app-id');
        const utilsMock = getUtilsMock();
        utilsMock.getStatsList.mockResolvedValue([{ '@id': 'test-table-1' }]); // Provide one table to process

        jest.doMock('apify', () => ({
            Actor: {
                main: jest.fn(async (fn) => fn()),
                getInput: mockGetInput,
                getValue: mockGetValue,
                pushData: jest.fn(),
            },
        }));

        jest.doMock('../src/utils.js', () => utilsMock);

        // --- 2. Execution ---
        const { mainLogic } = await import('../src/main.js');
        await mainLogic();

        // --- 3. Assertions ---
        expect(utilsMock.sleep).toHaveBeenCalledTimes(1);
        expect(utilsMock.sleep).toHaveBeenCalledWith(1500);
    });
  });
});
