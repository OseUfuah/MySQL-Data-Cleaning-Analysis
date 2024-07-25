##DATA ANALYSIS / EXPLORING##
-- Lay Off's: (LO's)

SELECT *
FROM layoffs_staging2;

-- Looking at highest LO's, and looking at highest percentage. Realized companys may have gone under
SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_staging2;

-- Comparing total LO's for companys that went under to see there size *(these are the only companys whose total size I can get a sense of since there is no column for company size)*
SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off =1
ORDER BY total_laid_off DESC;

-- Looking at LO's data time period (Ranges from around COVID impact, to early last year, 3 years)
SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2;

-- By comparing these 3 queries I can get a sense of; which company,industries, and countries got hit hardest at once, and over the duration to see the disparity.
SELECT company, MAX(total_laid_off), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 3 DESC;

SELECT industry, MAX(total_laid_off), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 3 DESC;

SELECT country, MAX(total_laid_off), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 3 DESC;

-- WOW, this data set only goes till March 6, 2023. Running this query tells me LO's have been ramping up serverily.
SELECT YEAR(`Date`), MAX(total_laid_off), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR(`Date`)
ORDER BY 1 DESC;

-- Looking at company development phase to see which types contributed to LO's most.
SELECT stage, MAX(total_laid_off), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY stage
ORDER BY 3 DESC;

-- Working a way of seeing LO's progression over time
SELECT YEAR(`Date`), MONTH(`Date`), SUM(total_laid_off), MAX(total_laid_off) 
FROM layoffs_staging2
WHERE MONTH(`Date`) IS NOT NULL
GROUP BY YEAR(`Date`), MONTH(`Date`)
ORDER BY 1 ASC;

-- Month by Month progression of Total LO'S (NOTED: in 2021 less people were let go, likely related to COVID as well)
WITH Rolling_Total AS
(
SELECT SUBSTRING(`date`,1,7) AS `Time_Period`, SUM(total_laid_off) AS LO_Sum, MAX(total_laid_off) AS LO_Max 
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `Time_Period`
ORDER BY 1 ASC
)
SELECT `Time_Period`, LO_Sum, SUM(LO_Sum) 
OVER(ORDER BY `Time_Period`) AS ROLLING_TOTAL
FROM Rolling_Total;

-- Taking a look at each company by year to see how many people the laid off.
SELECT company, YEAR(`Date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`Date`)
ORDER BY 3 DESC;

-- CTE thats ranks the top 10 companies with largest LO's by year per year
WITH Company_Year (company, years, total_laid_off) AS
(
SELECT company, YEAR(`Date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`Date`)
)
,Company_Ranking AS
(
SELECT *, DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
FROM Company_Year
WHERE years IS NOT NULL
ORDER BY RANKING ASC
)
SELECT *
FROM Company_Ranking
WHERE Ranking <=10 ;