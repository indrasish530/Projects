/*
Covid 19 Data Exploration using Snowflake SQL.
Skills used: Joins, CTE's, Temp Tables, Windows Functions, Aggregate Functions, Creating Views, Converting Data Types.
This data is from January 1,2020 to April 30,2021.
*/


create database portfolio_project;
use portfolio_project;


CREATE OR REPLACE TABLE COVID_DEATHS (
  iso_code VARCHAR(10),
  continent VARCHAR(20),
  location VARCHAR(100),
  date DATE,
  total_cases NUMBER(38,0),
  new_cases NUMBER(38,0),
  new_cases_smoothed NUMBER(38,0),
  total_deaths NUMBER(38,0),
  new_deaths NUMBER(38,0),
  new_deaths_smoothed NUMBER(38,0),
  total_cases_per_million NUMBER(38,6),
  new_cases_per_million NUMBER(38,6),
  new_cases_smoothed_per_million NUMBER(38,6),
  total_deaths_per_million NUMBER(38,6),
  new_deaths_per_million NUMBER(38,6),
  new_deaths_smoothed_per_million NUMBER(38,6),
  reproduction_rate NUMBER(38,4),
  icu_patients NUMBER(38,0),
  icu_patients_per_million NUMBER(38,6),
  hosp_patients NUMBER(38,0),
  hosp_patients_per_million NUMBER(38,6),
  weekly_icu_admissions NUMBER(38,0),
  weekly_icu_admissions_per_million NUMBER(38,6),
  weekly_hosp_admissions NUMBER(38,0),
  weekly_hosp_admissions_per_million NUMBER(38,6),
  new_tests NUMBER(38,0),
  total_tests NUMBER(38,0),
  total_tests_per_thousand NUMBER(38,6),
  new_tests_per_thousand NUMBER(38,6),
  new_tests_smoothed NUMBER(38,0),
  new_tests_smoothed_per_thousand NUMBER(38,6),
  positive_rate NUMBER(38,6),
  tests_per_case NUMBER(38,6),
  tests_units VARCHAR(20),
  total_vaccinations NUMBER(38,0),
  people_vaccinated NUMBER(38,0),
  people_fully_vaccinated NUMBER(38,0),
  new_vaccinations NUMBER(38,0),
  new_vaccinations_smoothed NUMBER(38,0),
  total_vaccinations_per_hundred NUMBER(38,6),
  people_vaccinated_per_hundred NUMBER(38,6),
  people_fully_vaccinated_per_hundred NUMBER(38,6),
  new_vaccinations_smoothed_per_million NUMBER(38,6),
  stringency_index NUMBER(38,4),
  population NUMBER(38,0),
  population_density NUMBER(38,6),
  median_age NUMBER(38,2),
  aged_65_older NUMBER(38,2),
  aged_70_older NUMBER(38,2),
  gdp_per_capita NUMBER(38,2),
  extreme_poverty NUMBER(38,2),
  cardiovasc_death_rate NUMBER(38,2),
  diabetes_prevalence NUMBER(38,2),
  female_smokers NUMBER(38,2),
  male_smokers NUMBER(38,2),
  handwashing_facilities NUMBER(38,2),
  hospital_beds_per_thousand NUMBER(38,2),
  life_expectancy NUMBER(38,2),
  human_development_index NUMBER(38,2)
);

SELECT * FROM COVID_DEATHS;


CREATE OR REPLACE TABLE COVID_VACCINATION (
  iso_code VARCHAR(10),
  continent VARCHAR(50),
  location VARCHAR(100),
  date DATE,
  new_tests NUMBER,
  total_tests NUMBER,
  total_tests_per_thousand NUMBER(18,3),
  new_tests_per_thousand NUMBER(18,3),
  new_tests_smoothed NUMBER,
  new_tests_smoothed_per_thousand NUMBER(18,3),
  positive_rate NUMBER(18,5),
  tests_per_case NUMBER(18,5),
  tests_units VARCHAR(50),
  total_vaccinations NUMBER,
  people_vaccinated NUMBER,
  people_fully_vaccinated NUMBER,
  new_vaccinations NUMBER,
  new_vaccinations_smoothed NUMBER,
  total_vaccinations_per_hundred NUMBER(18,3),
  people_vaccinated_per_hundred NUMBER(18,3),
  people_fully_vaccinated_per_hundred NUMBER(18,3),
  new_vaccinations_smoothed_per_million NUMBER,
  stringency_index NUMBER(18,3),
  population_density NUMBER(18,3),
  median_age NUMBER(18,3),
  aged_65_older NUMBER(18,3),
  aged_70_older NUMBER(18,3),
  gdp_per_capita NUMBER(18,3),
  extreme_poverty NUMBER(18,3),
  cardiovasc_death_rate NUMBER(18,3),
  diabetes_prevalence NUMBER(18,3),
  female_smokers NUMBER(18,3),
  male_smokers NUMBER(18,3),
  handwashing_facilities NUMBER(18,3),
  hospital_beds_per_thousand NUMBER(18,3),
  life_expectancy NUMBER(18,3),
  human_development_index NUMBER(18,3)
);


SELECT * FROM COVID_DEATHS;
SELECT * FROM COVID_VACCINATION;


-- Select Data that we are going to be starting with

Select continent,Location, date, total_cases, new_cases, total_deaths, population
From Covid_Deaths
order by 1,2;

------- Total Cases and Total Deaths in india

SELECT location,sum(total_cases) as total_cases,sum(total_deaths) as total_death,population
FROM COVID_DEATHS
where location in('India')
group by 1,4;



-- Total Cases vs Total Deaths
-- Shows likelihood of dying if you contract covid in your country

Select Location,date,population,total_cases,total_deaths,(total_deaths/total_cases)*100 as Death_percentage
From Covid_Deaths
where location in ('India') and 
death_percentage is not null
order by 6 desc;





-- Total Cases vs Population
-- Shows what percentage of population infected with Covid


Select Location,date,total_cases,total_deaths,population,(total_cases/population)*100 as Death_percentage
From Covid_Deaths
where location like '%India%' and 
death_percentage is not null
order by 6 desc;




-- Countries with Highest Infection Rate compared to Population

Select Location, Population, MAX(total_cases) as HighestInfectionCount,  Max((total_cases/population))*100 as PercentPopulationInfected
From Covid_Deaths
Group by 1,2
order by 4 desc;



-- Countries with Highest Death Count per Population

Select Location, MAX(Total_deaths) as TotalDeathCount
From Covid_Deaths
Group by 1
having totaldeathcount is not null
order by 2 desc;




--Continents with Highest Death Count per Population

Select continent, MAX(Total_deaths) as TotalDeathCount
From covid_deaths
Group by 1
having continent is not null 
order by 2 desc;




--Asian Countries Highest Death Count per Population


Select continent,Location, MAX(Total_deaths) as TotalDeathCount
From Covid_Deaths
where continent = 'Asia'
Group by 1,2
having totaldeathcount is not null
order by 3 desc ;




-- GLOBAL NUMBERS

Select SUM(new_cases) as total_cases, SUM(new_deaths) as total_deaths, SUM(new_deaths)/SUM(New_Cases)*100 as DeathPercentage
From Covid_Deaths
order by 1,2;




-- Total Population vs Vaccinations
-- Shows the Population that has recieved at least one Covid Vaccine

Select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations,
 SUM(vac.new_vaccinations) OVER (Partition by dea.Location Order by 2, 3) as RollingPeopleVaccinated
From Covid_Deaths as dea
Join Covid_Vaccination as vac
	On dea.location = vac.location
	and dea.date = vac.date
where dea.continent is not null 
order by 2,3 desc;




-- Total Population vs Vaccinations
-- Shows Percentage of Population that has recieved at least one Covid Vaccine


Select dea.continent, dea.location, dea.population, vac.new_vaccinations,
 (vac.new_vaccinations/dea.population)*100 as RollingPeopleVaccinated
From Covid_Deaths as dea
Join Covid_Vaccination as vac
	On dea.location = vac.location
	and dea.date = vac.date
where dea.continent is not null and RollingPeopleVaccinated is not null
group by 1,2,3,4
order by 3,5 desc;




-- Using CTE to perform Calculation on Partition By in previous query

With PopvsVac (Continent, Location, Date, Population, New_Vaccinations, RollingPeopleVaccinated)
as
(
Select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
, SUM(vac.new_vaccinations) OVER (Partition by dea.Location Order by dea.location, dea.Date) as RollingPeopleVaccinated
From Covid_Deaths dea
Join Covid_Vaccination vac
	On dea.location = vac.location
	and dea.date = vac.date
where dea.continent is not null 
  
)
Select *, (RollingPeopleVaccinated/Population)*100
From PopvsVac;





-- Using Temp Table to perform Calculation on Partition By in previous query

DROP Table if exists  PercentPopulationVaccinated;

Create or replace  Table PercentPopulationVaccinated
(
Continent nvarchar(255),
Location nvarchar(255),
Date date,
Population number,
New_vaccinations number,
RollingPeopleVaccinated number
);

Insert into PercentPopulationVaccinated
Select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations,
SUM(vac.new_vaccinations) OVER (Partition by dea.Location Order by dea.location, dea.Date) as RollingPeopleVaccinated
From Covid_Deaths as dea
Join Covid_Vaccination as vac
	On dea.location = vac.location
	and dea.date = vac.date;

select * from PercentPopulationVaccinated;

Select *, (RollingPeopleVaccinated/Population)*100
From PercentPopulationVaccinated;





-- Creating View to store data for later visualizations


Create View PercentPopulationVaccinatedview as
Select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations,
SUM(vac.new_vaccinations) OVER (Partition by dea.Location Order by dea.location, dea.Date) as RollingPeopleVaccinated
From Covid_Deaths as dea
Join Covid_Vaccination as vac
	On dea.location = vac.location
	and dea.date = vac.date
where dea.continent is not null ;


select * from PercentPopulationVaccinatedview;



-----creating view for visualizations in tableau

--1.

create or replace table total_death_count as
select SUM(new_cases) as total_cases, SUM(new_deaths) as total_deaths,SUM(new_deaths)/SUM(New_Cases)*100 as DeathPercentage
From Covid_Deaths
order by 1,2;

alter table total_death_count
add column location varchar(50);
update total_death_count
set location = 'World';

create or replace view total_death_count2 as
select * from total_death_count;

select * from total_death_count;





-- 2. 

-- We take these out as they are not inluded in the above queries and want to stay consistent
-- European Union is part of Europe
create or replace view continents_total_death as
Select location, SUM(new_deaths) as TotalDeathCount
From Covid_Deaths
Where continent is null 
and location not in ('World', 'European Union', 'International')
Group by 1
order by 2 desc;


select * from continents_total_death;

-- 3.removing all the null values for better visualizations

create or replace view total_percentage_infected_country as
Select Location, Population, MAX(total_cases) as HighestInfectionCount,  Max((total_cases/population))*100 as PercentPopulationInfected
From Covid_Deaths
Group by 1,2
having population is not null and HighestInfectionCount is not null and PercentPopulationInfected is not null
order by 3 desc;

select * from total_percentage_infected_country;



-- 4.removing all the null values for better visualizations


create or replace view highest_covid as
Select Location, Population,date, MAX(total_cases) as HighestInfectionCount,  Max((total_cases/population))*100 as PercentPopulationInfected
From Covid_Deaths
Group by 1,2,3
having HighestInfectionCount is not null and PercentPopulationInfected is not null
order by 4 desc;

select * from  highest_covid;


