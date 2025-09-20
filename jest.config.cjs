module.exports = {
  testEnvironment: 'node',
  verbose: true,
  testMatch: ['**/test/**/*.test.js'],
  transform: {
    '^.+\.js$': 'babel-jest',
  },
  moduleFileExtensions: ['js', 'json', 'node'],
  collectCoverage: true,
  coverageReporters: ['json', 'lcov', 'text', 'clover'],
  coverageDirectory: 'coverage',
  collectCoverageFrom: ['src/**/*.js'],
};
