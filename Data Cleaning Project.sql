##Data Cleaning##

# Creating Stage/Dev table from Main/Raw Data

CREATE TABLE layoffs_staging
LIKE layoffs_raw;

INSERT layoffs_staging
SELECT *
FROM layoffs_raw;

SELECT *
FROM layoffs_staging;
-- removing duplicates, standardize data, check nulls/blanks, remove unused columns and rows 
#Removing Dulpicates

-- Label How many of each row, (dups have >= 2)
SELECT *, 
ROW_NUMBER() 
OVER( PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) 
AS row_num
FROM layoffs_staging;

-- Pulls up dups (rows with >= 2)
WITH dupicate_labeler AS
(
SELECT *, 
ROW_NUMBER() 
OVER( PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) 
AS row_num
FROM layoffs_staging
)
SELECT *
FROM (dupicate_labeler) 
WHERE row_num > 1 ;

-- Create work around table with extra duplicate identifying column

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *, 
ROW_NUMBER() 
OVER( PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) 
AS row_num
FROM layoffs_staging;

-- Call dups on work around table 
SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

-- Delete the Dups
DELETE FROM layoffs_staging2 
WHERE
    row_num > 1;

#Standardizing the Data 

-- Getting rid of extra spaces in all columns,
SELECT *,  TRIM(company),TRIM( location), TRIM(industry), TRIM(total_laid_off), TRIM(percentage_laid_off), TRIM(`date`), TRIM(stage), TRIM(country), TRIM(funds_raised_millions)
FROM layoffs_staging2;

UPDATE layoffs_staging2 
SET company = TRIM(company);
UPDATE layoffs_staging2 
SET location = TRIM(location);
UPDATE layoffs_staging2 
SET industry = TRIM(industry);
UPDATE layoffs_staging2 
SET total_laid_off = TRIM(total_laid_off);
UPDATE layoffs_staging2 
SET percentage_laid_off = TRIM(percentage_laid_off);
UPDATE layoffs_staging2 
SET `date` = TRIM(`date`);
UPDATE layoffs_staging2 
SET stage = TRIM(stage);
UPDATE layoffs_staging2 
SET country = TRIM(country);
UPDATE layoffs_staging2 
SET funds_raised_millions = TRIM(funds_raised_millions);

-- Check for redundant labels industry
SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY industry;

		-- 3 variations of crypto currency noticed 
SELECT *
FROM layoffs_staging2
WHERE industry LIKE '%Crypto%'
ORDER BY industry;

					-- Crypto is now the universal label for that industry
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE '%Crypto%';

-- Check for redundant labels location
SELECT DISTINCT location
FROM layoffs_staging2
ORDER BY location;

-- Check for redundant labels country
SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY country;

		-- 2 variations of United States noticed (one with '.' at the end)
SELECT country
FROM layoffs_staging2
WHERE country LIKE '%United States%'
ORDER BY country;

					-- United States is now the universal label for that industry (no '.'s)
UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE '%United States%';

-- Convert date to time series format for charts
SELECT `date`, str_to_date(`date`,'%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = str_to_date(`date`,'%m/%d/%Y');

-- Set 'date' data type to date from text
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;


-- Standardize industry labels
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL OR industry = '';

-- Make Blanks NULLs for easier change
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- Pull named Industries of the same company/location
SELECT *
FROM layoffs_staging2 as t1
JOIN layoffs_staging2 as t2
	ON t1.company = t2.company
    AND t1.location = t2.location
WHERE (t1.industry IS NULL)
AND t2.industry IS NOT NULL;

-- Populate data into NULL values for to standardize Industry column
UPDATE layoffs_staging2 AS t1
JOIN layoffs_staging2 AS t2
	ON t1.company = t2.company
    AND t1.location = t2.location
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL)
AND t2.industry IS NOT NULL;

-- Delete Junk data with not real information (Optional)
DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;

-- Drop unused `column`
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

-- Final product of cleaned data
SELECT *
FROM layoffs_staging2


