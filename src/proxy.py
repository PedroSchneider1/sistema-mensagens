import zmq

context = zmq.Context()

# Publicadores (servidor) conectam aqui
xsub = context.socket(zmq.XSUB)
xsub.bind("tcp://*:5557")

# Inscritos (bots) conectam aqui
xpub = context.socket(zmq.XPUB)
xpub.bind("tcp://*:5558")

print("proxy: XSUB 5557 / XPUB 5558", flush=True)

try:
    zmq.proxy(xsub, xpub)
except KeyboardInterrupt:
    pass
finally:
    xsub.close()
    xpub.close()
    context.term()
