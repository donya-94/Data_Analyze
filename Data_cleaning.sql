-- Data Cleaning

Create table layoffs_staging 
like layoffs;


insert layoffs_staging 
select * from layoffs;

select * from layoffs_staging;





with duplicate_cte as (
select *,
row_number() over
	(partition by company, location, industry, 
		total_laid_off, percentage_laid_off, `date`, 
		stage, country, funds_raised_millions)
as row_num 
from layoffs_staging  #using `` for date because date is a key word in sql
)
select * from duplicate_cte where row_num > 1
;


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
  `raw_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


insert into layoffs_staging2 
select *,
row_number() over
	(partition by company, location, industry, 
		total_laid_off, percentage_laid_off, `date`, 
		stage, country, funds_raised_millions)
as row_num 
from layoffs_staging;  #using `` for date because date is a key word in sql


select * from layoffs_staging2;


delete from layoffs_staging2
where raw_num>1;



-- Standardizing data 

select distinct (trim(company)) from layoffs_staging2;

select company, trim(company) from layoffs_staging2;

update layoffs_staging2 set company = trim(company);

select distinct industry from layoffs_staging2
order by 1;

select * from layoffs_staging2 
where industry like 'crypto%';

update layoffs_staging2 
set industry = 'Crypto'
where industry like 'Crypto%';

select distinct country 
from layoffs_staging2
order by 1;

update layoffs_staging2 
set country = trim(trailing '.' from country) 
where country like 'United States%';

select `date` from layoffs_staging2;

alter table layoffs_staging2
modify column `date` date;

update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');


select * from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

select * from layoffs_staging2
where industry is null or industry = '';

select * from 
layoffs_staging2 t1 join layoffs_staging2 t2
on t1.company = t2.company
and t1.location = t2.location
where (t1.industry is null or t1.industry = '') and t2.industry is not null;

update layoffs_staging2 
set industry = null
where industry = '';

update layoffs_staging2 t1 join layoffs_staging2 t2
	on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null and t2.industry is not null;


select * from layoffs_staging2 
where total_laid_off is null and percentage_laid_off is null;

Delete from layoffs_staging2 
where total_laid_off is null and percentage_laid_off is null;


alter table layoffs_staging2
drop column raw_num;

select * from layoffs_staging2;


select company, sum(total_laid_off)
from layoffs_staging2
group by company
order by 2 desc; # 2 is for selecting second column which is sum(total_laid_of)

select min(`date`), max(`date`) from layoffs_staging2;