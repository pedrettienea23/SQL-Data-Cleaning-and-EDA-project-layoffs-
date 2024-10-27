SELECT * from layoffs;
-- Creating a copy of the dataset to avoid deleting values from the original dataset
CREATE TABLE layoffs_copy LIKE layoffs;
SELECT * FROM layoffs_copy;
INSERT layoffs_copy SELECT * FROM layoffs;
SELECT * FROM layoffs_copy;

-- Finding duplicates by comparing values
SELECT *,
ROW_NUMBER()
OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions)
AS row_num
FROM layoffs_copy;

WITH duplicates AS
(
SELECT *,
ROW_NUMBER()
OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_copy
)
SELECT * FROM duplicates WHERE row_num > 1;

-- Quick check
SELECT * FROM layoffs_copy WHERE company = 'Casper';

-- To delete only the duplicates, MySQL doesn't permit tu use DELETE. So, I am creating a new table with the 'row_num' column
CREATE TABLE `layoffs_copy2` (
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

INSERT INTO layoffs_copy2
SELECT *,
ROW_NUMBER()
OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions)
AS row_num
FROM layoffs_copy;

SELECT * FROM layoffs_copy2
WHERE row_num > 1;

DELETE FROM layoffs_copy2
WHERE row_num > 1;

SELECT * FROM layoffs_copy2
WHERE row_num > 1;

-- Standardizing data
UPDATE layoffs_copy2 -- Trimming the comany name
SET company = TRIM(company);

SELECT DISTINCT industry
FROM layoffs_copy2
ORDER BY 1; -- We can merge some Crypto industry under the same label
UPDATE layoffs_copy2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT location
FROM layoffs_copy2
ORDER BY 1; -- Looks good!

SELECT DISTINCT country
FROM layoffs_copy2
ORDER BY 1; -- Someone put a '.' after United States
UPDATE layoffs_copy2
SET country = 'United States'
WHERE country LIKE 'United States%';

-- Date columns is currently a text. Let's fix that
SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_copy2;
UPDATE layoffs_copy2 -- Changing format
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');
ALTER TABLE layoffs_copy2 -- Changing column format
MODIFY COLUMN  `date` DATE;

-- Missing values

-- Checking if there are companies with Null values in both total_laid_off and percentage_laid off
SELECT * FROM layoffs_copy2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL; -- There are Null values. What about blanks?

SELECT * FROM layoffs_copy2
WHERE industry = ''; -- Three companies have some blank values in industry. Can I repopulate it?

UPDATE layoffs_copy2
SET industry = NULL
WHERE industry = ''; -- Changin blanks into nulls

SELECT company, industry FROM layoffs_copy2
WHERE company = 'Airbnb'; -- We have values as "Travel" for Airbnb

-- Data repopulation
SELECT t1.industry, t2.industry
FROM layoffs_copy2 t1
JOIN layoffs_copy2 t2
	ON t1.company = t2.company
    AND t1.location = t2.location
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

UPDATE layoffs_copy2 t1
JOIN layoffs_copy2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- About other Null values in both total_laid_off and percentage_laid off, we can't repopulate them since we have no data to do so.
-- I think it is okay to delete them becasue I can't trust such a missing information. What if they didn't actaully have layoffs?

DELETE
FROM layoffs_copy2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Let's drop the old row_num column
ALTER TABLE layoffs_copy2
DROP COLUMN row_num;

SELECT * FROM layoffs_copy2;

-- Exploratory Data Analysis

SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_copy2; -- There is at least a company that laid off every employee

SELECT * FROM layoffs_copy2
WHERE percentage_laid_off = 1
ORDER BY total_laid_off DESC; -- There actually many of them

-- Wich company laid off more people?
SELECT company, SUM(total_laid_off)
FROM layoffs_copy2
GROUP BY COMPANY
ORDER BY 2 DESC;

-- Checking the time period of the dataset
SELECT MIN(`date`), MAX(`date`)
FROM layoffs_copy2; -- The minimun is around the pandemic time, and the maxmimum 3 years later

-- What industry laid off more people?
SELECT industry, SUM(total_laid_off)
FROM layoffs_copy2
GROUP BY industry
ORDER BY 2 DESC;

-- What country laid off more people?
SELECT country, SUM(total_laid_off)
FROM layoffs_copy2
GROUP BY country
ORDER BY 2 DESC;

-- Total layoffs per year
SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_copy2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;

-- What stage laid off more people?
SELECT stage, SUM(total_laid_off)
FROM layoffs_copy2
GROUP BY stage
ORDER BY 2 DESC;

-- Calculating the rolling sum of total layoffs month after month
SELECT SUBSTRING(`date`, 1, 7) AS `MONTH`, SUM(total_laid_off) 
FROM layoffs_copy2
WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC;

WITH Rolling_Total AS
(
SELECT SUBSTRING(`date`, 1, 7) AS `MONTH`, SUM(total_laid_off) AS total_off
FROM layoffs_copy2
WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC
)
SELECT `MONTH`, total_off,SUM(total_off) OVER(ORDER BY `MONTH`) AS rolling_total
FROM Rolling_Total;

-- What are the 5 companies that laid off most of the people each year?
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_copy2
GROUP BY company, YEAR(`date`)
ORDER BY 3 DESC;

WITH Company_Year (company, years, total_laid_off) AS
(
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_copy2
GROUP BY company, YEAR(`date`)
), company_year_Rank AS
(SELECT *, DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
FROM Company_Year
WHERE years IS NOT NULL
)
SELECT * FROM Company_Year_Rank
WHERE ranking <= 5;