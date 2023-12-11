package consumer;

class CountryPopulation {
    @Override
    public String toString() {
        return "CountryPopulation{" +
                       "country='" + country + '\'' +
                       ", population=" + population +
                       '}';
    }
    
    private String country;
    private Integer population;

    public CountryPopulation(String country, Integer population) {
        this.country = country;
        this.population = population;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public Integer getPopulation() {
        return population;
    }

    public void setPopulation(Integer population) {
        this.population = population;
    }
}