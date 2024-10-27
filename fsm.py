class PedidoFSM:
    # Definición de los estados y transiciones permitidas
    estados = {
        'Procesando': {'next': 'Preparación'},
        'Preparación': {'next': 'Enviado', 'previous': 'Procesando'},
        'Enviado': {'next': 'Entregado', 'previous': 'Preparación'},
        'Entregado': {'next': 'Finalizado', 'previous': 'Enviado'},
        'Finalizado': {'previous': 'Entregado'}
    }

    def __init__(self, estado_inicial='Procesando'):
        self.estado = estado_inicial

    def transicion(self, evento):
        if evento in self.estados[self.estado]:
            self.estado = self.estados[self.estado][evento]
        else:
            raise ValueError(f"Transición no válida desde el estado {self.estado} con evento '{evento}'")

    def estado_actual(self):
        return self.estado
