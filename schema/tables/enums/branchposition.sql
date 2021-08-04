DROP TYPE if EXISTS branchposition CASCADE;
CREATE TYPE branchposition as ENUM ('Top-of-canopy(TOC)', 'Mid(M)', 'Lower(L)');
