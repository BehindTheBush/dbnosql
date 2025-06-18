class Produto():
    
    def __init__(self):
        self.id = 0
        self.nome =  ""
        self.valor = 0.0

    def __str__(self):
        return f"Produto(id={self.id}, nome='{self.nome}', valor={self.valor})"