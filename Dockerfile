FROM continuumio/miniconda3:4.10.3-alpine

LABEL description="Base docker image with conda and dependencies"

WORKDIR /DataEngineeringProject

COPY environment.yml .

RUN conda env create --quiet --file ./environment.yml --name web_scraper python=3.7.9

RUN conda clean --all -f --yes

COPY . .

ENTRYPOINT ["conda", "run", "--no-capture-output", "-n", "web_scraper", "python"]

CMD ["./ScrapeService_Runner.py"]

